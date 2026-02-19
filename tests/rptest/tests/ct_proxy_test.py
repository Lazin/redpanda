# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from ducktape.utils.util import wait_until

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.ct_proxy import CtProxyService
from rptest.services.kgo_verifier_services import (
    KgoVerifierProducer,
    KgoVerifierSeqConsumer,
)
from rptest.services.redpanda import (
    CLOUD_TOPICS_CONFIG_STR,
    SISettings,
    make_redpanda_service,
)
from rptest.tests.redpanda_test import RedpandaTest


class CtProxySmokeTest(RedpandaTest):
    """
    Smoke test for ct-proxy that validates communication between
    Redpanda and ct-proxy via the epoch REST API endpoint.

    Prerequisites:
    - ct-proxy binary must be installed at {rp_install_path_root}/bin/ct-proxy
      or one of the standard paths (/opt/redpanda/bin, /usr/bin, etc.)
    - Cloud topics must be enabled in Redpanda
    """

    CLOUD_TOPIC_NAME = "ct-proxy-test-topic"

    def __init__(self, test_context):
        # Configure cloud topics
        extra_rp_conf = {
            CLOUD_TOPICS_CONFIG_STR: True,
            "enable_cluster_metadata_upload_loop": False,
        }

        # Set up cloud storage settings
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        super(CtProxySmokeTest, self).__init__(
            test_context=test_context,
            num_brokers=3,
            si_settings=si_settings,
            extra_rp_conf=extra_rp_conf,
        )

        self.rpk = RpkTool(self.redpanda)
        self.ct_proxy = None

    def setUp(self):
        super().setUp()
        # Create a cloud topic
        self.rpk.create_topic(
            topic=self.CLOUD_TOPIC_NAME,
            partitions=1,
            replicas=3,
            config={
                "redpanda.storage.mode": "cloud",
            },
        )
        self.logger.info(f"Created cloud topic: {self.CLOUD_TOPIC_NAME}")

    def tearDown(self):
        if self.ct_proxy is not None:
            try:
                self.ct_proxy.stop()
            except Exception as e:
                self.logger.warning(f"Error stopping ct-proxy: {e}")
        return super().tearDown()

    @cluster(num_nodes=4)  # 3 Redpanda + 1 ct-proxy
    def test_ct_proxy_epoch_api(self):
        """
        Test that ct-proxy can communicate with Redpanda by querying
        the cluster epoch for a cloud topic partition.
        """
        # Get cloud storage bucket from settings
        bucket = self.si_settings.cloud_storage_bucket

        # Start ct-proxy service on an extra node
        self.ct_proxy = CtProxyService(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.CLOUD_TOPIC_NAME,
            cloud_storage_bucket=bucket,
            cloud_storage_region=self.si_settings.cloud_storage_region,
            log_level="debug",
        )

        self.logger.info("Starting ct-proxy service")
        try:
            self.ct_proxy.start()
        except FileNotFoundError as e:
            self.logger.error(f"ct-proxy binary not found: {e}")
            raise RuntimeError(
                "ct-proxy binary not installed on test nodes. "
                "Ensure ct-proxy is built and deployed to the test environment. "
                "Build with: bazel build //:ct-proxy"
            ) from e

        # Wait for ct-proxy to be ready
        self.logger.info("Waiting for ct-proxy to become ready")
        self.ct_proxy.wait_ready(timeout_sec=60)

        self.logger.info(f"ct-proxy is ready at {self.ct_proxy.admin_url()}")

        # Query the epoch endpoint
        self.logger.info(
            f"Querying epoch for topic={self.CLOUD_TOPIC_NAME}, partition=0"
        )

        epoch = self.ct_proxy.get_epoch(
            topic=self.CLOUD_TOPIC_NAME,
            partition=0,
            timeout=30.0,
        )

        self.logger.info(f"Got cluster epoch: {epoch}")

        # The epoch should be a non-negative integer
        assert isinstance(epoch, int), f"Expected int, got {type(epoch)}"
        assert epoch >= 0, f"Expected non-negative epoch, got {epoch}"

        self.logger.info("ct-proxy smoke test passed")

    @cluster(num_nodes=4)  # 3 Redpanda + 1 ct-proxy
    def test_ct_proxy_list_topics_via_kafka(self):
        """
        Test that ct-proxy correctly proxies Kafka metadata requests.
        This test connects an RPK client to ct-proxy's Kafka port and
        lists topics to verify the Kafka protocol proxy is working.
        """
        # Get cloud storage bucket from settings
        bucket = self.si_settings.cloud_storage_bucket

        # Start ct-proxy service on an extra node
        self.ct_proxy = CtProxyService(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.CLOUD_TOPIC_NAME,
            cloud_storage_bucket=bucket,
            cloud_storage_region=self.si_settings.cloud_storage_region,
            log_level="debug",
        )

        self.logger.info("Starting ct-proxy service")
        try:
            self.ct_proxy.start()
        except FileNotFoundError as e:
            self.logger.error(f"ct-proxy binary not found: {e}")
            raise RuntimeError(
                "ct-proxy binary not installed on test nodes. "
                "Ensure ct-proxy is built and deployed to the test environment. "
                "Build with: bazel build //:ct-proxy"
            ) from e

        # Wait for ct-proxy to be ready
        self.logger.info("Waiting for ct-proxy to become ready")
        self.ct_proxy.wait_ready(timeout_sec=60)

        self.logger.info(
            f"ct-proxy is ready at {self.ct_proxy.admin_url()}, "
            f"Kafka endpoint: {self.ct_proxy.brokers()}"
        )

        # Create an RPK client that connects to ct-proxy instead of Redpanda
        rpk_via_proxy = RpkTool(self.ct_proxy)

        # List topics through ct-proxy
        self.logger.info("Listing topics via ct-proxy Kafka endpoint")
        topics = rpk_via_proxy.list_topics()

        self.logger.info(f"Topics returned via ct-proxy: {topics}")

        # Verify the cloud topic is visible through ct-proxy
        assert self.CLOUD_TOPIC_NAME in topics, (
            f"Expected topic '{self.CLOUD_TOPIC_NAME}' not found in topics: {topics}"
        )

        self.logger.info("ct-proxy Kafka list topics test passed")

    @cluster(num_nodes=4)  # 3 Redpanda + 1 ct-proxy
    def test_ct_proxy_produce_consume_via_kafka(self):
        """
        Test that ct-proxy correctly proxies Kafka produce and consume requests.
        This test:
        1. Starts ct-proxy
        2. Produces a message through ct-proxy's Kafka endpoint
        3. Consumes the message through ct-proxy's Kafka endpoint

        This validates the end-to-end L0 produce and consume path through ct-proxy.
        For cloud topics, ct-proxy:
        - On produce: creates L0 objects in S3 and replicates placeholders to Redpanda
        - On consume: reads placeholders from Redpanda and fetches L0 objects from S3
        """
        # Get cloud storage settings
        bucket = self.si_settings.cloud_storage_bucket
        endpoint_url = self.si_settings.endpoint_url
        access_key = self.si_settings.cloud_storage_access_key
        secret_key = self.si_settings.cloud_storage_secret_key

        self.logger.info(
            f"Cloud storage settings: bucket={bucket}, endpoint={endpoint_url}"
        )

        # Start ct-proxy service first
        self.ct_proxy = CtProxyService(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.CLOUD_TOPIC_NAME,
            cloud_storage_bucket=bucket,
            cloud_storage_region=self.si_settings.cloud_storage_region,
            cloud_storage_endpoint=endpoint_url,
            cloud_storage_access_key=access_key,
            cloud_storage_secret_key=secret_key,
            log_level="debug",
        )

        self.logger.info("Starting ct-proxy service")
        try:
            self.ct_proxy.start()
        except FileNotFoundError as e:
            self.logger.error(f"ct-proxy binary not found: {e}")
            raise RuntimeError(
                "ct-proxy binary not installed on test nodes. "
                "Ensure ct-proxy is built and deployed to the test environment. "
                "Build with: bazel build //:ct-proxy"
            ) from e

        # Wait for ct-proxy to be ready
        self.logger.info("Waiting for ct-proxy to become ready")
        self.ct_proxy.wait_ready(timeout_sec=60)

        self.logger.info(
            f"ct-proxy is ready at {self.ct_proxy.admin_url()}, "
            f"Kafka endpoint: {self.ct_proxy.brokers()}"
        )

        # Create an RPK client that connects to ct-proxy instead of Redpanda
        rpk_via_proxy = RpkTool(self.ct_proxy)
        rpk_directly = RpkTool(self.redpanda)

        # Produce a message through ct-proxy
        test_key = "test-key"
        test_value = "test-message-for-ct-proxy-produce-consume"

        self.logger.info(
            f"Producing message via Redpanda: "
            f"topic={self.CLOUD_TOPIC_NAME}, key={test_key}, value={test_value}"
        )
        rpk_directly.produce(
            topic=self.CLOUD_TOPIC_NAME,
            key=test_key,
            msg=test_value,
            partition=0,
        )
        self.logger.info("Message produced successfully via Redpanda")

        # Consume the message through ct-proxy
        self.logger.info("Consuming message via ct-proxy Kafka endpoint")
        consumed_output = rpk_via_proxy.consume(
            topic=self.CLOUD_TOPIC_NAME,
            n=1,
            offset="start",
            partition=0,
            timeout=30.0,
        )

        self.logger.info(f"Consumed output via ct-proxy: {consumed_output}")

        # Verify the message was consumed correctly
        assert test_value in consumed_output, (
            f"Expected message '{test_value}' not found in consumed output: "
            f"{consumed_output}"
        )

        self.logger.info("ct-proxy Kafka produce/consume test passed")

    @cluster(num_nodes=4)  # 3 Redpanda + 1 ct-proxy
    def test_ct_proxy_produce_via_proxy_consume_via_redpanda(self):
        """
        Test the ct-proxy write path by producing through ct-proxy and
        consuming directly from Redpanda.

        This test:
        1. Starts ct-proxy
        2. Produces a message through ct-proxy's Kafka endpoint
        3. Consumes the message directly from Redpanda

        This validates that ct-proxy correctly:
        - Creates L0 objects in S3
        - Replicates placeholders to Redpanda
        And that Redpanda can read back the L0 objects uploaded by ct-proxy.
        """
        # Get cloud storage settings
        bucket = self.si_settings.cloud_storage_bucket
        endpoint_url = self.si_settings.endpoint_url
        access_key = self.si_settings.cloud_storage_access_key
        secret_key = self.si_settings.cloud_storage_secret_key

        self.logger.info(
            f"Cloud storage settings: bucket={bucket}, endpoint={endpoint_url}"
        )

        # Start ct-proxy service
        self.ct_proxy = CtProxyService(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.CLOUD_TOPIC_NAME,
            cloud_storage_bucket=bucket,
            cloud_storage_region=self.si_settings.cloud_storage_region,
            cloud_storage_endpoint=endpoint_url,
            cloud_storage_access_key=access_key,
            cloud_storage_secret_key=secret_key,
            log_level="debug",
        )

        self.logger.info("Starting ct-proxy service")
        try:
            self.ct_proxy.start()
        except FileNotFoundError as e:
            self.logger.error(f"ct-proxy binary not found: {e}")
            raise RuntimeError(
                "ct-proxy binary not installed on test nodes. "
                "Ensure ct-proxy is built and deployed to the test environment. "
                "Build with: bazel build //:ct-proxy"
            ) from e

        # Wait for ct-proxy to be ready
        self.logger.info("Waiting for ct-proxy to become ready")
        self.ct_proxy.wait_ready(timeout_sec=60)

        self.logger.info(
            f"ct-proxy is ready at {self.ct_proxy.admin_url()}, "
            f"Kafka endpoint: {self.ct_proxy.brokers()}"
        )

        # Create RPK clients
        rpk_via_proxy = RpkTool(self.ct_proxy)
        rpk_directly = RpkTool(self.redpanda)

        # Produce a message through ct-proxy
        test_key = "test-key"
        test_value = "test-message-for-ct-proxy-write-path"

        self.logger.info(
            f"Producing message via ct-proxy: "
            f"topic={self.CLOUD_TOPIC_NAME}, key={test_key}, value={test_value}"
        )
        rpk_via_proxy.produce(
            topic=self.CLOUD_TOPIC_NAME,
            key=test_key,
            msg=test_value,
            partition=0,
        )
        self.logger.info("Message produced successfully via ct-proxy")

        # Consume the message directly from Redpanda
        self.logger.info("Consuming message directly from Redpanda")
        consumed_output = rpk_directly.consume(
            topic=self.CLOUD_TOPIC_NAME,
            n=1,
            offset="start",
            partition=0,
            timeout=30.0,
        )

        self.logger.info(f"Consumed output via Redpanda: {consumed_output}")

        # Verify the message was consumed correctly
        assert test_value in consumed_output, (
            f"Expected message '{test_value}' not found in consumed output: "
            f"{consumed_output}"
        )

        self.logger.info(
            "ct-proxy write path test passed: "
            "produce via ct-proxy, consume via Redpanda"
        )

    @cluster(num_nodes=6)  # 3 Redpanda + 1 ct-proxy + 1 producer + 1 consumer
    def test_ct_proxy_produce_consume_kgo_verifier(self):
        """
        Test ct-proxy produce and consume using KgoVerifier.

        Uses KgoVerifier to produce and consume multiple messages through
        ct-proxy's Kafka endpoint. This validates that ct-proxy correctly
        handles batched produce requests and sequential consumption with
        data integrity verification.
        """
        # Get cloud storage settings
        bucket = self.si_settings.cloud_storage_bucket
        endpoint_url = self.si_settings.endpoint_url
        access_key = self.si_settings.cloud_storage_access_key
        secret_key = self.si_settings.cloud_storage_secret_key

        # Start ct-proxy service
        self.ct_proxy = CtProxyService(
            context=self.test_context,
            redpanda=self.redpanda,
            topic=self.CLOUD_TOPIC_NAME,
            cloud_storage_bucket=bucket,
            cloud_storage_region=self.si_settings.cloud_storage_region,
            cloud_storage_endpoint=endpoint_url,
            cloud_storage_access_key=access_key,
            cloud_storage_secret_key=secret_key,
            log_level="debug",
        )

        self.logger.info("Starting ct-proxy service")
        try:
            self.ct_proxy.start()
        except FileNotFoundError as e:
            self.logger.error(f"ct-proxy binary not found: {e}")
            raise RuntimeError(
                "ct-proxy binary not installed on test nodes. "
                "Ensure ct-proxy is built and deployed to the test environment. "
                "Build with: bazel build //:ct-proxy"
            ) from e

        self.ct_proxy.wait_ready(timeout_sec=60)
        self.logger.info(
            f"ct-proxy is ready, Kafka endpoint: {self.ct_proxy.brokers()}"
        )

        msg_size = 1024
        msg_count = 100

        # Produce messages through ct-proxy using KgoVerifier
        producer = KgoVerifierProducer(
            self.test_context,
            self.ct_proxy,
            self.CLOUD_TOPIC_NAME,
            msg_size,
            msg_count,
        )
        producer.start()
        producer.wait(timeout_sec=60)
        producer.stop()

        self.logger.info(
            f"KgoVerifier producer finished: "
            f"sent={producer.produce_status.sent}, "
            f"acked={producer.produce_status.acked}, "
            f"bad_offsets={producer.produce_status.bad_offsets}"
        )

        assert producer.produce_status.acked == msg_count, (
            f"Expected {msg_count} acked, got {producer.produce_status.acked}"
        )

        # Consume messages through ct-proxy using KgoVerifier
        consumer = KgoVerifierSeqConsumer(
            self.test_context,
            self.ct_proxy,
            self.CLOUD_TOPIC_NAME,
            msg_size,
            max_msgs=msg_count,
            loop=False,
        )
        consumer.start(clean=True)
        consumer.wait(timeout_sec=60)
        consumer.stop()

        self.logger.info(
            f"KgoVerifier consumer finished: "
            f"valid_reads={consumer.consumer_status.validator.valid_reads}, "
            f"invalid_reads={consumer.consumer_status.validator.invalid_reads}"
        )

        assert consumer.consumer_status.validator.valid_reads >= msg_count, (
            f"Expected at least {msg_count} valid reads, "
            f"got {consumer.consumer_status.validator.valid_reads}"
        )
        assert consumer.consumer_status.validator.invalid_reads == 0, (
            f"Expected 0 invalid reads, "
            f"got {consumer.consumer_status.validator.invalid_reads}"
        )

        self.logger.info(
            "ct-proxy KgoVerifier produce/consume test passed"
        )
