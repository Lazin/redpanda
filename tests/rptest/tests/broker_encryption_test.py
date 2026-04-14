# Copyright 2024 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import json
import time

from ducktape.mark import ignore
from ducktape.tests.test import TestContext

from rptest.clients.rpk import RpkTool
from rptest.clients.types import TopicSpec
from rptest.services.cluster import cluster
from rptest.services.redpanda import (
    PandaproxyConfig,
    SchemaRegistryConfig,
)
from rptest.tests.redpanda_test import RedpandaTest

# Avro schema for a record containing a PII field (SSN) tagged for encryption
# via the schema registry metadata/ruleSet mechanism.
AVRO_SCHEMA_WITH_PII = json.dumps({
    "type":
    "record",
    "name":
    "UserRecord",
    "fields": [
        {
            "name": "user_id",
            "type": "string"
        },
        {
            "name": "name",
            "type": "string"
        },
        {
            "name": "ssn",
            "type": "string",
            "confluent:tags": ["PII"],
        },
    ],
})

# Schema registration payload with ENCRYPT rule targeting PII-tagged fields.
# The ruleSet instructs the broker to encrypt fields tagged as PII on produce
# and decrypt them on consume.
SCHEMA_WITH_ENCRYPT_RULE = json.dumps({
    "schema":
    AVRO_SCHEMA_WITH_PII,
    "schemaType":
    "AVRO",
    "metadata": {
        "properties": {
            "owner": "broker_encryption_test"
        }
    },
    "ruleSet": {
        "domainRules": [{
            "name": "encryptPII",
            "kind": "TRANSFORM",
            "mode": "WRITEREAD",
            "type": "ENCRYPT",
            "tags": ["PII"],
            "params": {
                "encrypt.kek.name": "test-kek",
                "encrypt.kms.type": "mock",
            },
        }]
    },
})

# Plain Avro schema with no encryption rules for the passthrough test.
AVRO_SCHEMA_NO_RULES = json.dumps({
    "type":
    "record",
    "name":
    "PlainRecord",
    "fields": [
        {
            "name": "user_id",
            "type": "string"
        },
        {
            "name": "payload",
            "type": "string"
        },
    ],
})

SCHEMA_WITHOUT_ENCRYPT_RULE = json.dumps({
    "schema": AVRO_SCHEMA_NO_RULES,
    "schemaType": "AVRO",
})


class BrokerEncryptionTest(RedpandaTest):
    """End-to-end broker-side field-level encryption (BSFLE) test.

    These tests verify that the broker transparently encrypts PII-tagged
    fields on produce and that the resulting records carry the expected
    encryption metadata headers.

    Prerequisites (not yet available):
      - Cluster-level BSFLE integration wired into the Kafka handler
      - Mock KMS cluster configuration
      - make_partition_proxy encryption path enabled

    All test methods are marked @ignore until the cluster integration is
    complete. The test bodies document the intended verification flow so
    that filling them in later is straightforward.
    """

    def __init__(self, test_context: TestContext):
        super().__init__(
            test_context,
            num_brokers=3,
            extra_rp_conf={
                # TODO: enable once the cluster config knob exists
                # "broker_side_field_level_encryption_enabled": True,
                # "bsfle_kms_provider": "mock",
            },
            schema_registry_config=SchemaRegistryConfig(),
            pandaproxy_config=PandaproxyConfig(),
        )
        self.rpk = RpkTool(self.redpanda)

    def _register_schema(self, subject: str, schema_data: str) -> int:
        """Register a schema via the schema registry REST API.

        Returns the schema ID assigned by the registry.
        """
        # TODO: implement once cluster integration is ready
        #
        # result = self.sr_client.post_subjects_subject_versions(
        #     subject=subject, data=schema_data
        # )
        # assert result.status_code == 200
        # return result.json()["id"]
        raise NotImplementedError("schema registration requires SR client")

    def _produce_records(
        self,
        topic: str,
        count: int,
        schema_id: int | None = None,
    ) -> None:
        """Produce `count` records with Avro-encoded user data.

        Each record has the form:
          key: "user-{i}"
          value: {"user_id": "u{i}", "name": "User {i}", "ssn": "123-45-{i:04d}"}
        """
        for i in range(count):
            value = json.dumps({
                "user_id": f"u{i}",
                "name": f"User {i}",
                "ssn": f"123-45-{i:04d}",
            })
            self.rpk.produce(
                topic,
                key=f"user-{i}",
                msg=value,
                schema_id=schema_id,
            )

    def _consume_records(
        self,
        topic: str,
        count: int,
    ) -> list[dict[str, str]]:
        """Consume `count` records and return them as parsed JSON dicts.

        Each returned dict contains at minimum the keys: topic, key, value,
        headers (as a JSON string).

        The rpk consume format string requests key, value, and headers so
        that the test can inspect encryption metadata.
        """
        # Use rpk consume with JSON output format to get headers
        output = self.rpk.consume(
            topic,
            n=count,
            offset="start",
            format=
            '{"key":"%k","value":"%v","headers":"%h","topic":"%t","partition":%p,"offset":%o}\n',
        )
        records = []
        for line in output.strip().splitlines():
            if line:
                records.append(json.loads(line))
        return records

    @cluster(num_nodes=3)
    @ignore  # BSFLE cluster integration pending
    def test_encrypted_produce_consume(self):
        """Produce plaintext records to a topic with an ENCRYPT rule and
        verify that the SSN field is encrypted in the stored records.

        Steps:
          1. Create a topic
          2. Register an Avro schema with a PII tag and ENCRYPT rule
          3. Produce 100 records with plaintext SSN values
          4. Consume all 100 records
          5. Verify each record carries the rp.encryption header
          6. Verify the SSN field value is NOT the original plaintext
          7. Verify non-PII fields (user_id, name) remain readable
        """
        topic = "bsfle-test-encrypted"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=SCHEMA_WITH_ENCRYPT_RULE,
        )

        record_count = 100
        self._produce_records(topic, record_count, schema_id=schema_id)

        records = self._consume_records(topic, record_count)
        assert len(records) == record_count

        for i, record in enumerate(records):
            # The encryption metadata header should be present
            assert "rp.encryption" in record.get("headers", ""), (
                f"record {i}: missing rp.encryption header"
            )

            # The SSN field must not appear as the original plaintext
            original_ssn = f"123-45-{i:04d}"
            value = record.get("value", "")
            assert original_ssn not in value, (
                f"record {i}: SSN field was not encrypted, "
                f"found plaintext '{original_ssn}' in value"
            )

            # Non-PII fields should still be readable (they are not
            # encrypted). Parse the value if it is valid JSON after
            # decoding the Avro envelope.
            # TODO: implement value deserialization once the wire
            # format is finalized

    @cluster(num_nodes=3)
    @ignore  # BSFLE cluster integration pending
    def test_dek_rotation(self):
        """Produce records, wait for DEK expiry, produce again, and verify
        that the two batches use different DEK versions.

        Steps:
          1. Create a topic with a short DEK expiry (e.g. 5 seconds)
          2. Produce batch A (50 records)
          3. Sleep past the DEK expiry window
          4. Produce batch B (50 records)
          5. Consume all 100 records
          6. Extract dek_version from the rp.encryption header
          7. Verify batch A and batch B have different dek_version values
        """
        topic = "bsfle-test-dek-rotation"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=SCHEMA_WITH_ENCRYPT_RULE,
        )

        batch_a_count = 50
        self._produce_records(topic, batch_a_count, schema_id=schema_id)

        # Sleep past DEK expiry. The actual expiry is configured via
        # cluster config once the integration is wired up; for this
        # skeleton we assume a short TTL.
        dek_expiry_seconds = 5
        time.sleep(dek_expiry_seconds + 2)

        batch_b_count = 50
        self._produce_records(
            topic,
            batch_b_count,
            schema_id=schema_id,
        )

        total = batch_a_count + batch_b_count
        records = self._consume_records(topic, total)
        assert len(records) == total

        # TODO: parse the rp.encryption header to extract dek_version
        # and verify that batch A versions differ from batch B versions.
        #
        # dek_versions_a = {
        #     parse_encryption_header(r)["dek_version"]
        #     for r in records[:batch_a_count]
        # }
        # dek_versions_b = {
        #     parse_encryption_header(r)["dek_version"]
        #     for r in records[batch_a_count:]
        # }
        # assert dek_versions_a != dek_versions_b, (
        #     "DEK versions should differ after rotation"
        # )

    @cluster(num_nodes=3)
    @ignore  # BSFLE cluster integration pending
    def test_no_encryption_passthrough(self):
        """Produce records to a topic whose schema has no ENCRYPT rule and
        verify that records pass through without modification.

        Steps:
          1. Create a topic
          2. Register an Avro schema WITHOUT encryption rules
          3. Produce 50 records
          4. Consume all 50 records
          5. Verify NO rp.encryption header is present
          6. Verify all field values match the original plaintext
        """
        topic = "bsfle-test-passthrough"
        self.rpk.create_topic(topic, partitions=1, replicas=3)

        schema_id = self._register_schema(
            subject=f"{topic}-value",
            schema_data=SCHEMA_WITHOUT_ENCRYPT_RULE,
        )

        record_count = 50
        # Produce records using the plain schema helper; re-use the
        # same produce helper (the ssn field exists but should not be
        # encrypted because no ENCRYPT rule is registered).
        self._produce_records(topic, record_count, schema_id=schema_id)

        records = self._consume_records(topic, record_count)
        assert len(records) == record_count

        for i, record in enumerate(records):
            # No encryption header should be present
            assert "rp.encryption" not in record.get("headers", ""), (
                f"record {i}: unexpected rp.encryption header "
                f"on topic without encryption rules"
            )

            # The original field values should be present verbatim
            original_ssn = f"123-45-{i:04d}"
            value = record.get("value", "")
            assert original_ssn in value, (
                f"record {i}: expected plaintext SSN '{original_ssn}' "
                f"in value but it was not found"
            )
