import boto3
import copy
from botocore.config import Config
from botocore.exceptions import ClientError
from time import sleep
from functools import wraps
from collections import namedtuple
import time
import datetime
from typing import Union
import json
import struct
import collections
import xxhash

HDR_FMT_RP = "<IiqbIhiqqqhii"

HEADER_SIZE = struct.calcsize(HDR_FMT_RP)

Header = collections.namedtuple(
    'Header', ('header_crc', 'batch_size', 'base_offset', 'type', 'crc',
               'attrs', 'delta', 'first_ts', 'max_ts', 'producer_id',
               'producer_epoch', 'base_seq', 'record_count'))


class SegmentReader:
    def __init__(self, stream):
        self.stream = stream

    def read_batch(self):
        data = self.stream.read(HEADER_SIZE)
        if len(data) == HEADER_SIZE:
            header = Header(*struct.unpack(HDR_FMT_RP, data))
            if all(map(lambda v: v == 0, header)):
                return None
            records_size = header.batch_size - HEADER_SIZE
            data = self.stream.read(records_size)
            if len(data) < records_size:
                return None
            assert len(data) == records_size
            return header
        return None

    def __iter__(self):
        while True:
            it = self.read_batch()
            if it is None:
                return
            yield it


def get_segment_path(namespace, topic, id, rev, segment_name, term):
    x = xxhash.xxh32()
    path = f"{namespace}/{topic}/{id}_{rev}/{segment_name}"
    x.update(path.encode('ascii'))
    hash = x.hexdigest()
    if term is None:
        return f"{hash}/{path}"
    else:
        return f"{hash}/{path}.{term}"


NTP = namedtuple('NTP', ['ns', 'topic', 'partition', 'rev'])

def get_partition_manifest_path(ntp):
    x = xxhash.xxh32()
    path = f"{ntp.ns}/{ntp.topic}/{ntp.partition}_{ntp.rev}"
    x.update(path.encode('ascii'))
    manifest_hash = x.hexdigest()[0] + '0000000'
    return f"{manifest_hash}/meta/{path}/manifest.json"


class SlowDown(Exception):
    pass


S3ObjectMetadata = namedtuple('S3ObjectMetadata',
                              ['Bucket', 'Key', 'ETag', 'ContentLength'])


def retry_on_slowdown(tries=4, delay=1.0, backoff=2.0):
    """Retry with an exponential backoff if SlowDown exception was triggered."""
    def retry(fn):
        @wraps(fn)
        def do_retry(*args, **kwargs):
            ntries, sec_delay = tries, delay
            while ntries > 1:
                try:
                    return fn(*args, **kwargs)
                except SlowDown:
                    sleep(sec_delay)
                    ntries -= 1
                    sec_delay *= backoff
            # will stop filtering SlowDown exception when quota is reache
            return fn(*args, **kwargs)

        return do_retry

    return retry


class S3Client:
    """Simple S3 client"""
    def __init__(self,
                 region,
                 access_key,
                 secret_key,
                 logger,
                 endpoint=None,
                 disable_ssl=True):
        cfg = Config(region_name=region, signature_version='s3v4')
        self._cli = boto3.client('s3',
                                 config=cfg,
                                 aws_access_key_id=access_key,
                                 aws_secret_access_key=secret_key,
                                 endpoint_url=endpoint,
                                 use_ssl=not disable_ssl)
        self._region = region
        self.logger = logger

    def create_bucket(self, name):
        """Create bucket in S3"""
        try:
            res = self._cli.create_bucket(
                Bucket=name,
                CreateBucketConfiguration={'LocationConstraint': self._region})

            self.logger.debug(res)
            assert res['ResponseMetadata']['HTTPStatusCode'] == 200
        except ClientError as err:
            if err.response['Error']['Code'] != 'BucketAlreadyOwnedByYou':
                raise err

    def delete_bucket(self, name):
        try:
            self._cli.delete_bucket(Bucket=name)
        except Exception:
            self.logger.warn("Error deleting bucket, contents:")
            for o in self.list_objects(name):
                self.logger.warn(f"  {o.Key}")
            raise

    def empty_bucket(self, name):
        """Empty bucket, return list of keys that wasn't deleted due
        to error"""
        keys = []
        try:
            self.logger.debug(f"running bucket cleanup on {name}")
            for obj in self.list_objects(bucket=name):
                keys.append(obj.Key)
                self.logger.debug(f"found key {obj.Key}")
        except Exception as e:
            # Expected to fail if bucket doesn't exist
            self.logger.debug(f"empty_bucket error: {e}")

        failed_keys = []
        while len(keys):
            # S3 API supports up to 1000 keys per delete request
            batch = keys[0:1000]
            keys = keys[1000:]
            self.logger.debug(f"deleting keys {batch[0]}..{batch[-1]}")
            try:
                reply = self._cli.delete_objects(
                    Bucket=name,
                    Delete={'Objects': [{
                        'Key': k
                    } for k in batch]})
                self.logger.debug(f"delete request reply: {reply}")
            except:
                self.logger.exception(
                    f"Delete request failed for keys {batch[0]}..{batch[-1]}")
                failed_keys.extend(batch)
        return failed_keys

    def delete_object(self, bucket, key, verify=False):
        """Remove object from S3"""
        res = self._delete_object(bucket, key)
        if verify:
            self._wait_no_key(bucket, key)
        return res

    def _wait_no_key(self, bucket, key, timeout_sec=10):
        """Wait for the key to apper in the bucket"""
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        try:
            # Busy wait until the object is actually
            # deleted
            while True:
                self._head_object(bucket, key)
                # aws boto3 uses 5s interval for polling
                # using head_object API call
                now = datetime.datetime.now()
                if now > deadline:
                    raise TimeoutError()
                time.sleep(5)
        except ClientError as err:
            self.logger.debug(f"error response while polling {err}")

    def _wait_key(self, bucket, key, timeout_sec=30):
        """Wait for the key to apper in the bucket"""
        # Busy wait until the object is available
        deadline = datetime.datetime.now() + datetime.timedelta(
            seconds=timeout_sec)
        while True:
            try:
                meta = self._head_object(bucket, key)
                self.logger.debug(f"object {key} is available, head: {meta}")
                return
            except ClientError as err:
                self.logger.debug(f"error response while polling {err}")
            now = datetime.datetime.now()
            if now > deadline:
                raise TimeoutError()
            # aws boto3 uses 5s interval for polling
            # using head_object API call
            time.sleep(5)

    @retry_on_slowdown()
    def _delete_object(self, bucket, key):
        """Remove object from S3"""
        try:
            return self._cli.delete_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _get_object(self, bucket, key):
        """Get object from S3"""
        try:
            return self._cli.get_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _head_object(self, bucket, key):
        """Get object from S3"""
        try:
            return self._cli.head_object(Bucket=bucket, Key=key)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _put_object(self, bucket, key, content):
        """Put object to S3"""
        try:
            return self._cli.put_object(Bucket=bucket,
                                        Key=key,
                                        Body=bytes(content, encoding='utf-8'))
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    @retry_on_slowdown()
    def _copy_single_object(self, bucket, src, dst):
        """Copy object to another location within the bucket"""
        try:
            src_uri = f"{bucket}/{src}"
            return self._cli.copy_object(Bucket=bucket,
                                         Key=dst,
                                         CopySource=src_uri)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    def get_object_data(self, bucket, key):
        resp = self._get_object(bucket, key)
        return resp['Body'].read()

    def get_object_data_stream(self, bucket, key):
        resp = self._get_object(bucket, key)
        return resp['Body']

    def put_object(self, bucket, key, data):
        self._put_object(bucket, key, data)

    def copy_object(self,
                    bucket,
                    src,
                    dst,
                    validate=False,
                    validation_timeout_sec=30):
        """Copy object inside a bucket and optionally poll the destination until
        the copy will be available or timeout passes."""
        self._copy_single_object(bucket, src, dst)
        if validate:
            self._wait_key(bucket, dst, validation_timeout_sec)

    def move_object(self,
                    bucket,
                    src,
                    dst,
                    validate=False,
                    validation_timeout_sec=30):
        """Move object inside a bucket and optionally poll the destination until
        the new location will be available and old will disappear or timeout is reached."""
        self._copy_single_object(bucket, src, dst)
        self._delete_object(bucket, src)
        if validate:
            self._wait_key(bucket, dst, validation_timeout_sec)
            self._wait_no_key(bucket, src, validation_timeout_sec)

    def get_object_meta(self, bucket, key):
        """Get object metadata without downloading it"""
        resp = self._get_object(bucket, key)
        # Note: ETag field contains md5 hash enclosed in double quotes that have to be removed
        return S3ObjectMetadata(Bucket=bucket,
                                Key=key,
                                ETag=resp['ETag'][1:-1],
                                ContentLength=resp['ContentLength'])

    def write_object_to_file(self, bucket, key, dest_path):
        """Get object and write it to file"""
        resp = self._get_object(bucket, key)
        with open(dest_path, 'wb') as f:
            body = resp['Body']
            for chunk in body.iter_chunks(chunk_size=0x1000):
                f.write(chunk)

    @retry_on_slowdown()
    def _list_objects(self, bucket, token=None, limit=1000):
        try:
            if token is not None:
                return self._cli.list_objects_v2(
                    Bucket=bucket,
                    #MaxKeys=limit,
                    ContinuationToken=token)
            else:
                return self._cli.list_objects_v2(
                    Bucket=bucket)  #, MaxKeys=limit)
        except ClientError as err:
            self.logger.debug(f"error response {err}")
            if err.response['Error']['Code'] == 'SlowDown':
                raise SlowDown()
            else:
                raise

    def list_objects(self, bucket):
        token = None
        truncated = True
        while truncated:
            res = self._list_objects(bucket, token, limit=100)
            token = res.get('NextContinuationToken')
            truncated = bool(res['IsTruncated'])
            if 'Contents' in res:
                for item in res['Contents']:
                    yield S3ObjectMetadata(Bucket=bucket,
                                           Key=item['Key'],
                                           ETag=item['ETag'][1:-1],
                                           ContentLength=item['Size'])

    def list_buckets(self) -> dict[str, Union[list, dict]]:
        try:
            return self._cli.list_buckets()
        except Exception as ex:
            self.logger.error(f'Error listing buckets: {ex}')
            raise


class ManifestIterator:
    def __init__(self, obj):
        self.data = obj
        self.namespace = obj['namespace']
        self.topic = obj['topic']
        self.partition = obj['partition']
        self.revision = obj['revision']
        segments = []
        for key, value in obj['segments'].items():
            segments.append(
                (key, value.get('archiver_term'), value.get('ntp_revision')))
        self.segments = sorted(segments, key=lambda x: int(x[0].split('-')[0]))

    def list_segments(self):
        for key, term, rev in self.segments:
            yield key, get_segment_path(self.namespace, self.topic,
                                        self.partition, rev or self.revision,
                                        key, term)


SegmentMeta = collections.namedtuple(
    'SegmentMeta', ('base_offset', 'base_timestamp', 'last_offset',
                    'max_timestamp', 'num_configs', 'total_size'))


def get_segment_meta(s3client: S3Client, bucket, path):
    print(f"Downloading segment {path}")
    stream = s3client.get_object_data_stream(bucket, path)
    rdr = SegmentReader(stream)
    total_size = 0
    num_configs = 0
    base_offset = None
    base_timestamp = None
    max_timestamp = -1
    last_offset = 0
    for batch in rdr:
        if batch is None:
            break
        if batch.type == 2 or batch.type == 19:  # raft configuration or archival metadata stm
            num_configs += batch.delta + 1
        max_timestamp = max(max_timestamp, batch.first_ts)
        if base_timestamp is None:
            base_timestamp = batch.first_ts
        if base_offset is None:
            base_offset = batch.base_offset
        total_size += batch.batch_size
        last_offset = max(last_offset, batch.base_offset + batch.record_count)

    return SegmentMeta(base_offset=base_offset,
                       base_timestamp=base_timestamp,
                       last_offset=last_offset,
                       max_timestamp=max_timestamp,
                       num_configs=num_configs,
                       total_size=total_size)

def main():
    import argparse
    import logging

    logger = logging.getLogger("main")

    def generate_options():
        parser = argparse.ArgumentParser(description='List remote partitions')

        parser.add_argument('region', type=str, help='name of the AWS region')
        parser.add_argument('bucket', type=str, help='bucket name')
        parser.add_argument('access_key', type=str, help='access key')
        parser.add_argument('secret_key', type=str, help='secret key')
        # partition parameters
        parser.add_argument('namespace', type=str, help='namespace')
        parser.add_argument('topic', type=str, help='topic name')
        parser.add_argument('partition', type=str, help='partition id')
        parser.add_argument('revision', type=str, help='revision id')
        # API endpoint
        parser.add_argument('--endpoint', type=str, help='endpoint (default: AWS S3)', default=None)
        return parser

    parser = generate_options()
    options, _ = parser.parse_known_args()

    client = S3Client(options.region, options.access_key, options.secret_key,
                      logger, options.endpoint)

    ntp = NTP(ns=options.namespace, 
              topic=options.topic, 
              partition=options.partition, 
              rev=options.revision)

    manifest_path = get_partition_manifest_path(ntp)
    manifest_data = client.get_object_data(options.bucket, manifest_path)
    manifest_json = json.loads(manifest_data)
    manifest = ManifestIterator(manifest_json)

    delta_offset = 0
    partition_manifest = copy.deepcopy(manifest_json)
    for key, path in manifest.list_segments():
        meta = get_segment_meta(client, options.bucket, path)
        partition_manifest['segments'][key]['delta_offset'] = delta_offset
        delta_offset += meta.num_configs

    print(f"Uploading output to {manifest_path}")
    client.put_object(options.bucket, manifest_path, json.dumps(partition_manifest))
    
    fname = manifest_path.replace('/', '_')
    print(f"writing output to {fname}")
    json.dump(partition_manifest, open(fname, 'w+'))


if __name__ == '__main__':
    main()
