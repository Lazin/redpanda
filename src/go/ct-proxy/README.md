# ct-proxy - Cloud Topics Kafka Proxy for Redpanda

ct-proxy is a Kafka protocol proxy that allows external applications to interact with Redpanda cloud topics by handling the data plane (uploading/downloading L0 objects to/from cloud storage) while delegating the metadata plane (placeholder replication, epoch tracking) to Redpanda's admin API.

## Architecture

```
Kafka Client <-> ct-proxy (Go) <-> Redpanda Admin API (C++)
                    |                        |
                    v                        v
                 S3 Bucket             Cloud Topics L0/L1
```

### Write Path
1. Client sends produce request to ct-proxy
2. ct-proxy gets current cluster epoch from Redpanda admin API
3. ct-proxy creates L0 object and uploads to S3
4. ct-proxy replicates placeholder to Redpanda (with RW-fence)
5. ct-proxy acknowledges to client

### Read Path
1. Client sends fetch request to ct-proxy
2. ct-proxy reads placeholders from Redpanda admin API
3. ct-proxy downloads L0 objects from S3
4. ct-proxy materializes records and returns to client

## Components

### Redpanda Admin APIs (C++)

Located in `src/v/redpanda/admin/services/internal/ct_proxy_service.{h,cc}`

**5 New Admin APIs:**

1. **GetClusterEpoch**: Returns current cluster epoch for L0 object naming
2. **ReplicatePlaceholders**: Replicates placeholders with RW-fencing
3. **ReadPlaceholders**: Reads placeholders from partition log
4. **ListCloudTopicPartitions**: Lists all cloud topic partitions
5. **ReadL1Metadata**: Reads L1 metastore extent metadata

### Go ct-proxy Application

Located in `src/go/ct-proxy/`

**Key Packages:**

- `pkg/config/` - YAML configuration
- `pkg/l0/` - L0 object serialization (critical - matches Redpanda serde format)
- `pkg/admin/` - Admin API client wrapper
- `pkg/proxy/` - Producer, consumer, and metadata handlers
- `pkg/storage/` - S3 upload/download client

## Building

### Prerequisites

```bash
# Install Bazelisk
wget -O ~/bin/bazel https://github.com/bazelbuild/bazelisk/releases/latest/download/bazelisk-linux-amd64
chmod +x ~/bin/bazel
export PATH="$HOME/bin:$PATH"

# Install system dependencies
sudo ./bazel/install-deps.sh
```

### Build Commands

```bash
# Build C++ admin API service
bazel build //src/v/redpanda/admin/services/internal:ct_proxy_service

# Test C++ service
bazel test //src/v/redpanda/admin/services/internal:ct_proxy_service_test

# Build Go ct-proxy binary
bazel build //src/go/ct-proxy:ct-proxy

# Test Go packages
bazel test //src/go/ct-proxy/pkg/l0:l0_test
bazel test //src/go/ct-proxy/pkg/config:config_test
bazel test //src/go/ct-proxy/pkg/admin:admin_test
bazel test //src/go/ct-proxy/pkg/proxy:proxy_test
```

## Configuration

Create a `config.yaml` file (see `config.yaml.example` for reference):

```yaml
server:
  kafka_listen_address: "0.0.0.0:9092"
  admin_listen_address: "0.0.0.0:9644"

redpanda:
  admin_api:
    addresses:
      - "localhost:9644"
    tls:
      enabled: false

cloud_storage:
  provider: "aws"  # aws, gcp, or azure
  region: "us-west-2"
  bucket: "my-cloud-topics-bucket"

cloud_topics:
  allowed_topics:
    - "my-cloud-topic-1"
    - "my-cloud-topic-2"

logging:
  level: "info"
  format: "json"
```

### Environment Variables

For AWS S3:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_SESSION_TOKEN` (optional)

For GCP:
- `GOOGLE_APPLICATION_CREDENTIALS`

For Azure:
- `AZURE_STORAGE_ACCOUNT`
- `AZURE_STORAGE_KEY`

## Running

### Start Redpanda with Cloud Topics

```bash
# Redpanda must be configured with cloud topics enabled
# and S3 storage configured
```

### Start ct-proxy

```bash
./bazel-bin/src/go/ct-proxy/ct-proxy --config config.yaml
```

### Use with Kafka Clients

```bash
# Produce to cloud topic via ct-proxy
echo "test message" | kcat -P -b localhost:9092 -t my-cloud-topic-1

# Consume from cloud topic via ct-proxy
kcat -C -b localhost:9092 -t my-cloud-topic-1
```

## Design Constraints

To prioritize simplicity:

1. **No Aggregation**: One produce request = one S3 object
2. **No Caching**: Downloads fetch from S3 every time
3. **No Transactions**: Transactional producers are rejected
4. **No Idempotency**: Idempotent producers (without aggregation) are rejected
5. **Single Broker**: ct-proxy presents itself as a single Kafka broker
6. **Allowed Topics Only**: Only configured topics are accessible

## Critical Implementation Details

### L0 Serialization Format

The placeholder serialization in `pkg/l0/placeholder.go` **MUST** match Redpanda's C++ serde::envelope format exactly:

```
Outer envelope (ctp_placeholder):
  - version: 4 bytes (little endian) = 0
  - compat_version: 4 bytes (little endian) = 0
Inner envelope (object_id):
  - version: 4 bytes (little endian) = 1
  - compat_version: 4 bytes (little endian) = 0
  - epoch: 8 bytes (little endian)
  - uuid: 16 bytes (raw)
  - prefix: 2 bytes (little endian)
Placeholder fields:
  - first_byte_offset: 8 bytes (little endian)
  - byte_range_size: 8 bytes (little endian)
Total: 58 bytes
```

### L0 Object Path Format

```
level_zero/data/{prefix:03}/{epoch:018}/{uuid}
```

Example: `level_zero/data/042/000000000000012345/550e8400-e29b-41d4-a716-446655440000`

## Testing

### Unit Tests

```bash
# Run all Go tests
bazel test //src/go/ct-proxy/...

# Run critical L0 serialization tests
bazel test //src/go/ct-proxy/pkg/l0:l0_test

# Run C++ tests
bazel test //src/v/redpanda/admin/services/internal:ct_proxy_service_test
```

### Integration Test

1. Start Redpanda with cloud topics enabled
2. Create a cloud topic: `rpk topic create test-ct --config cloud.topic=true`
3. Start ct-proxy: `./ct-proxy --config config.yaml`
4. Produce via ct-proxy: `echo "test" | kcat -P -b localhost:9092 -t test-ct`
5. Verify S3 object: `aws s3 ls s3://my-bucket/level_zero/data/`
6. Consume via ct-proxy: `kcat -C -b localhost:9092 -t test-ct`

### Compatibility Test

- Produce via ct-proxy → consume via Redpanda (validates L0 format)
- Produce via Redpanda → consume via ct-proxy (validates placeholder parsing)

## Monitoring

ct-proxy logs all operations in structured JSON format:

```json
{
  "level": "info",
  "ts": "2025-02-09T12:00:00Z",
  "msg": "replicated placeholder",
  "topic": "my-cloud-topic-1",
  "partition": 0,
  "last_offset": 100,
  "term": 1
}
```

Recommended metrics to track:
- Produce requests per second
- Fetch requests per second
- S3 upload/download bytes
- Placeholder replication latency
- Admin API call latency

## Limitations

1. **Not Production-Ready**: This is a reference implementation
2. **No High Availability**: Single instance only
3. **No Load Balancing**: No partition reassignment
4. **Limited Error Handling**: Basic error responses
5. **No Kafka Protocol Completeness**: Only essential APIs implemented
6. **L0 Only**: L1 metadata API not fully implemented

## Future Enhancements

To make this production-ready:

1. Implement full Kafka record batch encoding/decoding
2. Add connection pooling for S3 and admin API
3. Implement proper Kafka protocol wire format handling
4. Add comprehensive error handling and retries
5. Implement L1 metadata API
6. Add metrics and observability
7. Support multiple ct-proxy instances with coordination
8. Add TLS support for admin API
9. Implement request batching and caching

## References

- [Redpanda Documentation](https://redpanda.com/documentation)
- [Cloud Topics Architecture](src/v/cloud_topics/)
- [Admin API Services](src/v/redpanda/admin/services/)
- [Franz-go Kafka Client](https://github.com/twmb/franz-go)

## License

Licensed under the Redpanda Community License (RCL).
See [licenses/rcl.md](../../../licenses/rcl.md) for details.
