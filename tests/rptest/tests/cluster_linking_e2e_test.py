# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import ducktape.errors
import google.protobuf.timestamp_pb2
import google.protobuf.duration_pb2
import google.protobuf.field_mask_pb2
import random
import re
import threading
import time
import json

from ducktape.cluster.cluster import ClusterNode
from ducktape.cluster.cluster_spec import ClusterSpec
from connectrpc.errors import ConnectError, ConnectErrorCode
from ducktape.mark import matrix

from rptest.clients.admin.proto.redpanda.core.common.v1 import acl_pb2, tls_pb2
from rptest.clients.admin.proto.redpanda.core.admin.v2 import (
    shadow_link_pb2,
)
from rptest.clients.kafka_cli_tools import KafkaCliToolsError
from rptest.clients.rpk import (
    RpkPartition,
    RpkTool,
    RPKACLInput,
    RpkException,
    RpkGroup,
)
from rptest.clients.types import TopicSpec
from rptest.services.cluster import TestContext
from rptest.services.admin import Admin
from rptest.services.cluster import cluster
from rptest.services.kgo_verifier_services import (
    KgoVerifierConsumerGroupConsumer,
    KgoVerifierProducer,
)
from rptest.services.multi_cluster_services import (
    Cluster,
    RedpandaCluster,
    MultiClusterServices,
    SecondaryClusterArgs,
    SecondaryClusterSpec,
    ServiceType,
    Service as MultiService,
)
from rptest.services.redpanda import (
    RESTART_LOG_ALLOW_LIST,
    MetricSamples,
    MetricsEndpoint,
    RedpandaService,
    SchemaRegistryConfig,
    SecurityConfig,
    SISettings,
)
from rptest.services.tls import TLSCertManager
from rptest.tests.cluster_linking_test_base import (
    ALL_STORAGE_MODES,
    CLOUD_TOPICS_SHADOW_LINK_LOG_ALLOW_LIST,
    CONTROLLER_LOCKED_TASKS,
    DEFAULT_SYNCED_TOPIC_PROPERTIES,
    DISALLOWED_SYNCED_TOPIC_PROPERTIES,
    REQUIRED_SYNCED_TOPIC_PROPERTIES,
    ClusterLinkingTLSProvider,
    ShadowLinkPreAllocTestBase,
    ShadowLinkTestBase,
)
from rptest.tests.full_disk_test import FDT_LOG_ALLOW_LIST
from rptest.tests.redpanda_test import RedpandaTest
from rptest.util import (
    expect_exception,
    wait_until,
    wait_until_result,
)
from rptest.utils.full_disk import FullDiskHelper
from typing import Any, Callable, Optional
from time import sleep
import google.protobuf.duration_pb2

FDT_CL_REPLICATION_REJECTION = [
    re.compile(
        ".*Error in fetch_and_replicate.*no disk space; free bytes less than configurable threshold\\)"
    )
]


class MultiClusterTestBase(RedpandaTest):
    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, *args, **kwargs)

    def basic_ops(self, services: MultiClusterServices):
        def at_least_one_topic_exists(services: MultiClusterServices, node: Cluster):
            topics = services.list_topics(node, detailed=True)
            return len(topics) > 0, topics

        topic = "test-topic"
        services.create_topic(services.primary, topic, partitions=3, replicas=3)
        p_topics = wait_until_result(
            lambda: at_least_one_topic_exists(services, services.primary),
            timeout_sec=30,
            err_msg="Failed to create a single topic on the primary cluster",
        )

        services.create_topic(services.secondary, topic, partitions=3, replicas=3)
        s_topics = wait_until_result(
            lambda: at_least_one_topic_exists(services, services.secondary),
            timeout_sec=30,
            err_msg="Failed to create a single topic on the secondary cluster",
        )

        assert p_topics == s_topics, (
            f"Expected same topics on both clusters, got {p_topics=} vs {s_topics=}"
        )

        assert len(p_topics) == 1 and p_topics[0][0] == topic, (
            f"Expected {topic=}, got {p_topics=}"
        )

        status_json = services.primary.admin.get_status_ready()
        assert status_json["status"] == "ready", f"Expected ready, got {status_json=}"

        if services.secondary.is_redpanda:
            status_json = services.secondary.admin.get_status_ready()
            assert status_json["status"] == "ready", (
                f"Expected ready, got {status_json=}"
            )
        else:
            with expect_exception(NotImplementedError, lambda e: True):
                services.secondary.admin.get_status_ready()


class MultiClusterRedpandaTest(MultiClusterTestBase):
    """
    Just verifies MultiClusterServices for now. rp + rp & rp + kafka
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, num_brokers=3, *args, **kwargs)

        self.test_context = test_context

    def setUp(self):
        # MultiClusterServices will set itself up
        pass

    @cluster(num_nodes=6)
    def test_basic_ops(self):
        with MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_spec=SecondaryClusterSpec(ServiceType.REDPANDA),
        ) as services:
            assert services.secondary.is_redpanda, (
                f"Expected Redpanda service, got {services.secondary}"
            )
            self.basic_ops(services)


class MultiClusterKafkaTest(MultiClusterTestBase):
    """
    Just verifies MultiClusterServices for now. rp + rp & rp + kafka
    """

    def __init__(self, test_context, *args, **kwargs):
        super().__init__(test_context=test_context, num_brokers=3, *args, **kwargs)

        self.test_context = test_context

    def setUp(self):
        # MultiClusterServices will set itself up
        pass

    @cluster(num_nodes=6)
    def test_basic_ops(self):
        with MultiClusterServices(
            self.test_context,
            self.logger,
            self.redpanda,
            secondary_spec=SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ) as services:
            assert services.secondary.is_kafka, (
                f"Expected Kafka service, got {services.secondary}"
            )
            self.basic_ops(services)


class ShadowLinkBasicTests(ShadowLinkTestBase):
    def _expect_connect_error(self, expected_code: ConnectErrorCode):
        return expect_exception(ConnectError, lambda e: e.code == expected_code)

    def _schema_registry_api_sync_options(
        self,
    ) -> shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi:
        return shadow_link_pb2.SchemaRegistrySyncOptions.ShadowSchemaRegistryApi(
            source_url="http://schema-registry.example.com:8081"
        )

    def _topics_are_present_in_target_cluster(self, topics):
        target_rpk = RpkTool(self.target_cluster.service)
        topics_in_target = {t for t in target_rpk.list_topics()}
        self.logger.info(f"Topics in target cluster: {topics_in_target}")
        if len(topics_in_target) < len(topics):
            return False
        for t in topics:
            if t.name not in topics_in_target:
                return False

        return True

    @cluster(num_nodes=6)
    def test_schema_registry_api_sync_rejected_when_feature_inactive(self):
        self.target_cluster_service.set_feature_active("shadow_link_sr_api_sync", False)

        create_req = self.create_default_link_request(
            link_name="sr-api-link",
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        create_req.shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            self._schema_registry_api_sync_options()
        )

        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.create_link_with_request(req=create_req)

        shadow_link = self.create_link(
            "test-link",
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        shadow_link.configurations.schema_registry_sync_options.shadow_schema_registry_api.CopyFrom(
            self._schema_registry_api_sync_options()
        )
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.schema_registry_sync_options"]
        )

        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

    @cluster(num_nodes=6)
    def test_role_sync_rejected_when_feature_inactive(self):
        self.target_cluster_service.set_feature_active("shadow_link_role_sync", False)

        role_sync_options = shadow_link_pb2.RoleSyncOptions(
            role_name_filters=[
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="app-",
                )
            ]
        )

        create_req = self.create_default_link_request(
            link_name="role-sync-link",
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        create_req.shadow_link.configurations.role_sync_options.CopyFrom(
            role_sync_options
        )

        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.create_link_with_request(req=create_req)

        shadow_link = self.create_link(
            "test-link",
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        shadow_link.configurations.role_sync_options.CopyFrom(role_sync_options)
        update_mask = google.protobuf.field_mask_pb2.FieldMask(
            paths=["configurations.role_sync_options"]
        )

        with self._expect_connect_error(ConnectErrorCode.FAILED_PRECONDITION):
            self.update_link(shadow_link=shadow_link, update_mask=update_mask)

    @cluster(num_nodes=6)
    def test_create_default_link(self):
        """
        This test creates a Shadow Link with all default values and
        verifies that the default values are what are in use
        """
        link_request = self.create_default_link_request(
            link_name="test-link",
            mirror_all_acls=False,
            mirror_all_groups=False,
            mirror_all_topics=False,
        )
        link_request.shadow_link.configurations.topic_metadata_sync_options.interval.CopyFrom(
            google.protobuf.duration_pb2.Duration(seconds=0)
        )
        link_request.shadow_link.configurations.consumer_offset_sync_options.interval.CopyFrom(
            google.protobuf.duration_pb2.Duration(seconds=0)
        )
        link_request.shadow_link.configurations.security_sync_options.interval.CopyFrom(
            google.protobuf.duration_pb2.Duration(seconds=0)
        )

        shadow_link = self.create_link_with_request(req=link_request)

        self.logger.info(f"Shadow link configurations: {shadow_link.configurations}")

        client_options = shadow_link.configurations.client_options
        assert client_options.metadata_max_age_ms == 0, (
            f"Expected 0, got {client_options.metadata_max_age_ms}"
        )
        assert client_options.effective_metadata_max_age_ms == 10000, (
            f"Expected 10000, got {client_options.effective_metadata_max_age_ms}"
        )
        assert client_options.connection_timeout_ms == 0, (
            f"Expected 0, got {client_options.connection_timeout_ms}"
        )
        assert client_options.effective_connection_timeout_ms == 1000, (
            f"Expected 1000, got {client_options.effective_connection_timeout_ms}"
        )
        assert client_options.retry_backoff_ms == 0, (
            f"Expected 0, got {client_options.retry_backoff_ms}"
        )
        assert client_options.effective_retry_backoff_ms == 100, (
            f"Expected 100, got {client_options.effective_retry_backoff_ms}"
        )
        assert client_options.fetch_wait_max_ms == 0, (
            f"Expected 0, got {client_options.fetch_wait_max_ms}"
        )
        assert client_options.effective_fetch_wait_max_ms == 500, (
            f"Expected 500, got {client_options.effective_fetch_wait_max_ms}"
        )
        assert client_options.fetch_min_bytes == 0, (
            f"Expected 0, got {client_options.fetch_min_bytes}"
        )
        assert client_options.effective_fetch_min_bytes == (5 * 1024 * 1024), (
            f"Expected {5 * 1024 * 1024}, got {client_options.effective_fetch_min_bytes}"
        )
        assert client_options.fetch_max_bytes == 0, (
            f"Expected 0, got {client_options.fetch_max_bytes}"
        )
        assert client_options.effective_fetch_max_bytes == (20 * 1024 * 1024), (
            f"Expected {20 * 1024 * 1024}, got {client_options.effective_fetch_max_bytes}"
        )
        assert client_options.fetch_partition_max_bytes == 0, (
            f"Expected 0, got {client_options.fetch_partition_max_bytes}"
        )
        assert client_options.effective_fetch_partition_max_bytes == (
            5 * 1024 * 1024
        ), (
            f"Expected {5 * 1024 * 1024}, got {client_options.effective_fetch_partition_max_bytes}"
        )

        topic_metadata_config = shadow_link.configurations.topic_metadata_sync_options
        assert topic_metadata_config.interval == google.protobuf.duration_pb2.Duration(
            seconds=0
        ), f"Expected 0s, got {topic_metadata_config.interval}"
        assert (
            topic_metadata_config.effective_interval
            == google.protobuf.duration_pb2.Duration(seconds=30)
        ), f"Expected 30s, got {topic_metadata_config.effective_interval}"

        cg_config = shadow_link.configurations.consumer_offset_sync_options
        assert cg_config.interval == google.protobuf.duration_pb2.Duration(seconds=0), (
            f"Expected 0s, got {cg_config.interval}"
        )
        assert cg_config.effective_interval == google.protobuf.duration_pb2.Duration(
            seconds=30
        ), f"Expected 30s, got {cg_config.effective_interval}"

        security_config = shadow_link.configurations.security_sync_options
        assert security_config.interval == google.protobuf.duration_pb2.Duration(
            seconds=0
        ), f"Expected 0s, got {security_config.interval}"
        assert (
            security_config.effective_interval
            == google.protobuf.duration_pb2.Duration(seconds=30)
        ), f"Expected 30s, got {security_config.effective_interval}"

    @cluster(num_nodes=6)
    def test_create_simple_link(self):
        shadow_link = self.create_link("test-link")
        self.logger.info(f"Create shadow link result: {shadow_link}")

        links = self.list_links()
        assert len(links) == 1, f"Expected exactly one shadow link, got {len(links)}"

        test_link = links[0]
        assert test_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {test_link.name}"
        )

        active = shadow_link_pb2.ShadowLinkState.SHADOW_LINK_STATE_ACTIVE
        link_state = test_link.status.state
        assert link_state == active, (
            f"Expected shadow link state to be '{active}', got {link_state}"
        )

        link_uid = test_link.uid
        assert link_uid, "Expected some uid for shadow link"

        got_link = self.get_link(name="test-link")
        assert got_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {got_link.name}"
        )

        assert got_link.uid == link_uid, (
            f"Expected shadow link uid to be '{link_uid}', got {got_link.uid}"
        )

        # Retrieving a non-existent link should fail
        with self._expect_connect_error(ConnectErrorCode.NOT_FOUND):
            self.get_link(name="non-existent-link")

        task_statuses = got_link.status.task_statuses
        self.logger.info(f"Shadow link task_statuses: {task_statuses}")

        # Get the controller leader
        leader_id = Admin(self.target_cluster_service).get_partition_leader(
            namespace="redpanda", topic="controller", partition=0
        )

        for task in task_statuses:
            if task.name in CONTROLLER_LOCKED_TASKS:
                assert task.state == shadow_link_pb2.TASK_STATE_ACTIVE, (
                    f'Expected task "{task.name}" to be running, got {task.state}'
                )
                assert task.broker_id == leader_id, (
                    f'Expected task "{task.name}" to be running on controller node {leader_id} not {task.broker_id}'
                )
                assert task.shard == 0, (
                    f'Expected task "{task.name}" to be running on shard 0 not {task.shard}'
                )

    @cluster(num_nodes=6)
    def test_task_states_change(self):
        topic = TopicSpec(name="test-topic", partition_count=3, replication_factor=3)
        self.source_default_client().create_topic(topic)
        req = self.create_default_link_request("test-link")
        req.shadow_link.configurations.role_sync_options.CopyFrom(
            shadow_link_pb2.RoleSyncOptions(
                interval=google.protobuf.duration_pb2.Duration(seconds=1),
                role_name_filters=[
                    shadow_link_pb2.NameFilter(
                        pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                        filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                        name="e2e-roles-",
                    )
                ],
            )
        )
        self.create_link_with_request(req)

        wait_until(
            lambda: self._topics_are_present_in_target_cluster([topic]),
            timeout_sec=20,
            err_msg="Failed to find topic in target cluster",
        )

        def _wait_for_controller_tasks_state(
            expected_state: shadow_link_pb2.TaskState.ValueType,
        ) -> bool:
            # Get the controller leader
            leader_id = Admin(self.target_cluster_service).get_partition_leader(
                namespace="redpanda", topic="controller", partition=0
            )
            task_statuses = self.get_link("test-link").status.task_statuses
            self.logger.debug(f"Task statuses: {task_statuses}")
            for task in task_statuses:
                if task.name in CONTROLLER_LOCKED_TASKS:
                    assert task.broker_id == leader_id, (
                        f'Expected task "{task.name}" to be running on controller node {leader_id} not {task.broker_id}'
                    )
                    assert task.shard == 0, (
                        f'Expected task "{task.name}" to be running on shard 0 not {task.shard}'
                    )
                    if task.state != expected_state:
                        return False
            return True

        wait_until(
            lambda: _wait_for_controller_tasks_state(shadow_link_pb2.TASK_STATE_ACTIVE),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Controller locked tasks did not become active",
        )

        # Now shut down the source cluster
        self.source_cluster.stop()

        wait_until(
            lambda: _wait_for_controller_tasks_state(
                shadow_link_pb2.TASK_STATE_LINK_UNAVAILABLE
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Controller locked tasks did not become link unavailable",
        )

        # Now restart and expect things to recover
        self.source_cluster.start()
        wait_until(
            lambda: _wait_for_controller_tasks_state(shadow_link_pb2.TASK_STATE_ACTIVE),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Controller locked tasks did not become active after source cluster restart",
        )

    @cluster(num_nodes=6)
    def test_can_not_create_more_than_one_link(self):
        shadow_link = self.create_link("test-link")

        assert shadow_link.name == "test-link", (
            f"Expected shadow link name to be 'test-link', got {shadow_link.name}"
        )

        # Attempting to create a second one with the same name should fail
        with self._expect_connect_error(ConnectErrorCode.ALREADY_EXISTS):
            self.create_link("test-link")

        # Attempting to create a second link should fail.
        # Only one link is supported per cluster
        with self._expect_connect_error(ConnectErrorCode.RESOURCE_EXHAUSTED):
            self.create_link("test-link-2")

    @cluster(num_nodes=6)
    def test_topic_creation_in_target_cluster(self):
        topics = []
        for i in range(10):
            cleanup_policy = "delete" if i % 2 == 0 else "compact"
            topic = TopicSpec(
                name=f"source-topic-{i}",
                partition_count=i + 3,
                replication_factor=3,
                cleanup_policy=cleanup_policy,
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        self.create_link("test-link")

        wait_until(
            lambda: self._topics_are_present_in_target_cluster(topics),
            timeout_sec=20,
            err_msg="Failed to find topics in the target cluster",
        )
        target_rpk = RpkTool(self.target_cluster.service)
        for t in topics:
            target_configs = target_rpk.describe_topic_configs(t.name)
            self.logger.info(f"Target topic {t.name} configs: {target_configs}")
            assert target_configs["cleanup.policy"][0] == t.cleanup_policy, (
                f"Expected cleanup policy {t.cleanup_policy} for topic {t.name}, "
                f"got {target_configs['cleanup.policy']}"
            )

        shadow_topics = self.list_shadow_topics(shadow_link_name="test-link")
        assert len(shadow_topics) == len(topics), (
            f"Expected {len(topics)} shadow topics, got {len(shadow_topics)}"
        )

        for t in topics:
            found = False
            for st in shadow_topics:
                if st.name == t.name:
                    found = True
                    break
            assert found, f"Did not find shadow topic for {t.name}"

        for t in topics:
            self.get_shadow_topic(
                shadow_link_name="test-link", shadow_topic_name=t.name
            )

        with self._expect_connect_error(ConnectErrorCode.NOT_FOUND):
            self.get_shadow_topic(
                shadow_link_name="test-link", shadow_topic_name="non-existent-topic"
            )

    @cluster(num_nodes=6)
    def test_topic_creation_restriction(self):
        """
        Test validates that when cluster linking is active, that topics can only be created by superusers
        """
        username = "test-user"
        password = "test-password0"
        topic_name_prefix = "test-topic"

        superuser_rpk = RpkTool(
            self.target_cluster_service,
            username=self.redpanda.SUPERUSER_CREDENTIALS.username,
            password=self.redpanda.SUPERUSER_CREDENTIALS.password,
            sasl_mechanism=self.redpanda.SUPERUSER_CREDENTIALS.mechanism,
        )
        normaluser_rpk = RpkTool(
            self.target_cluster_service,
            username=username,
            password=password,
            sasl_mechanism="SCRAM-SHA-256",
        )

        self.logger.debug(f'Creating user "{username}"')
        superuser_rpk.sasl_create_user(new_username=username, new_password=password)
        new_acl = RPKACLInput()
        new_acl.allow_principal = [f"User:{username}"]
        new_acl.operation = ["ALL"]
        new_acl.resource_pattern_type = "prefixed"
        new_acl.topic = [topic_name_prefix]

        self.logger.debug("Enabling SASL on target cluster")

        self.target_cluster_service.set_cluster_config(values={"enable_sasl": True})

        self.logger.debug(f"Creating ACL {new_acl}")
        superuser_rpk.acl_create(acl=new_acl)

        # Verifying that a normal user can create a topic without link being present
        normaluser_rpk.create_topic(f"{topic_name_prefix}-1")

        self.logger.debug("Creating cluster link")
        self.create_link("test-link")

        # Now verify that the user cannot create the topic
        try:
            normaluser_rpk.create_topic(f"{topic_name_prefix}-2")
            assert False, "Should not have been able to create a topic"
        except RpkException:
            pass

        superuser_rpk.create_topic(f"{topic_name_prefix}-3")

    @cluster(num_nodes=6)
    def test_update_link(self):
        """
        This is a simple test to verify that the UpdateShadowLink API works.

        First the test creates 10 topics on the source cluster, then it creates
        a shadow link with no topic filters

        It then verifies that no topics were created, then updates the shadow
        link to add two topic filters: one to select all by prefix and one to
        exclude literally

        Then it verifies that the included topics are replicated and the excluded
        topic is not
        """
        topic_prefix = "source-topic-"
        topics: list[TopicSpec] = []
        for i in range(10):
            topic = TopicSpec(
                name=f"{topic_prefix}{i}", partition_count=3, replication_factor=3
            )
            self.source_default_client().create_topic(topic)
            topics.append(topic)

        shadow_link: shadow_link_pb2.ShadowLink = self.create_link(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        def _any_topics_are_present_in_target_cluster():
            topics_in_target = {t for t in self.target_cluster_rpk.list_topics()}
            for t in topics:
                if t.name in topics_in_target:
                    return True

            return False

        with expect_exception(ducktape.errors.TimeoutError, lambda _: True):
            wait_until(_any_topics_are_present_in_target_cluster, timeout_sec=5)

        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name=topic_prefix,
                ),
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_LITERAL,
                    filter_type=shadow_link_pb2.FILTER_TYPE_EXCLUDE,
                    name=f"{topic_prefix}0",
                ),
            ]
        )
        shadow_link.configurations.client_options.fetch_wait_max_ms = 100
        shadow_link.configurations.client_options.fetch_min_bytes = 10
        shadow_link.configurations.client_options.fetch_partition_max_bytes = (
            500 * 1024 * 1024
        )
        shadow_link.configurations.client_options.metadata_max_age_ms = 500
        shadow_link.configurations.client_options.connection_timeout_ms = 100
        shadow_link.configurations.client_options.retry_backoff_ms = 200
        shadow_link.configurations.client_options.fetch_max_bytes = 100 * 1024 * 1024
        update_mask: google.protobuf.field_mask_pb2.FieldMask = google.protobuf.field_mask_pb2.FieldMask(
            paths=[
                "configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters",
                "configurations.client_options.fetch_partition_max_bytes",
                "configurations.client_options.fetch_wait_max_ms",
                "configurations.client_options.fetch_min_bytes",
                "configurations.client_options.metadata_max_age_ms",
                "configurations.client_options.connection_timeout_ms",
                "configurations.client_options.retry_backoff_ms",
                "configurations.client_options.fetch_max_bytes",
            ]
        )

        updated_link = self.update_link(
            shadow_link=shadow_link, update_mask=update_mask
        )

        assert (
            updated_link.configurations.topic_metadata_sync_options
            == shadow_link.configurations.topic_metadata_sync_options
        ), (
            f"Expected updated link to be returned, {updated_link.configurations.topic_metadata_sync_options} != {shadow_link.configurations.topic_metadata_sync_options}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_wait_max_ms
            == shadow_link.configurations.client_options.fetch_wait_max_ms
        ), (
            f"Expected fetch_wait_max_ms to be {shadow_link.configurations.client_options.fetch_wait_max_ms}, got {updated_link.configurations.client_options.effective_fetch_wait_max_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_min_bytes
            == shadow_link.configurations.client_options.fetch_min_bytes
        ), (
            f"Expected fetch_min_bytes to be {shadow_link.configurations.client_options.fetch_min_bytes}, got {updated_link.configurations.client_options.effective_fetch_min_bytes}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_partition_max_bytes
            == shadow_link.configurations.client_options.fetch_partition_max_bytes
        ), (
            f"Expected fetch_partition_max_bytes to be {shadow_link.configurations.client_options.fetch_partition_max_bytes}, got {updated_link.configurations.client_options.effective_fetch_partition_max_bytes}"
        )
        assert (
            updated_link.configurations.client_options.effective_metadata_max_age_ms
            == shadow_link.configurations.client_options.metadata_max_age_ms
        ), (
            f"Expected metadata_max_age_ms to be {shadow_link.configurations.client_options.metadata_max_age_ms}, got {updated_link.configurations.client_options.effective_metadata_max_age_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_connection_timeout_ms
            == shadow_link.configurations.client_options.connection_timeout_ms
        ), (
            f"Expected connection_timeout_ms to be {shadow_link.configurations.client_options.connection_timeout_ms}, got {updated_link.configurations.client_options.effective_connection_timeout_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_retry_backoff_ms
            == shadow_link.configurations.client_options.retry_backoff_ms
        ), (
            f"Expected retry_backoff_ms to be {shadow_link.configurations.client_options.retry_backoff_ms}, got {updated_link.configurations.client_options.effective_retry_backoff_ms}"
        )
        assert (
            updated_link.configurations.client_options.effective_fetch_max_bytes
            == shadow_link.configurations.client_options.fetch_max_bytes
        ), (
            f"Expected fetch_max_bytes to be {shadow_link.configurations.client_options.fetch_max_bytes}, got {updated_link.configurations.client_options.effective_fetch_max_bytes}"
        )

        def _all_but_one_topic_are_present_in_target_cluster():
            topics_in_target = {t for t in self.target_cluster_rpk.list_topics()}
            found_count = 0
            for t in topics:
                if t.name in topics_in_target:
                    if t.name == f"{topic_prefix}0":
                        assert False, f"{topic_prefix}0 should not be mirrored!"
                    found_count += 1

            self.logger.info(f"{found_count} == {len(topics) - 1}")
            return found_count == (len(topics) - 1)

        wait_until(
            _all_but_one_topic_are_present_in_target_cluster,
            timeout_sec=20,
            backoff_sec=1,
            err_msg="Not all topics were mirrored",
        )

    @cluster(num_nodes=6)
    def test_update_not_in_field_mask(self):
        shadow_link: shadow_link_pb2.ShadowLink = self.create_link(
            "test-link", mirror_all_topics=False, mirror_all_groups=False
        )

        shadow_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters.extend(
            [
                shadow_link_pb2.NameFilter(
                    pattern_type=shadow_link_pb2.PATTERN_TYPE_PREFIX,
                    filter_type=shadow_link_pb2.FILTER_TYPE_INCLUDE,
                    name="*",
                ),
            ]
        )
        expected_duration = google.protobuf.duration_pb2.Duration(seconds=600)
        shadow_link.configurations.topic_metadata_sync_options.interval.CopyFrom(
            expected_duration
        )

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.topic_metadata_sync_options.interval"]
            )
        )

        updated_link = self.update_link(
            shadow_link=shadow_link, update_mask=update_mask
        )

        assert (
            updated_link.configurations.topic_metadata_sync_options.interval
            == expected_duration
        ), (
            f"Expected duration to be {expected_duration}, got {updated_link.configurations.topic_metadata_sync_options.interval}"
        )

        assert (
            len(
                updated_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters
            )
            == 0
        ), (
            f"Expected topic filters to not be updated, got {updated_link.configurations.topic_metadata_sync_options.auto_create_shadow_topic_filters}"
        )

    @cluster(
        num_nodes=6,
        log_allow_list=CLOUD_TOPICS_SHADOW_LINK_LOG_ALLOW_LIST,
    )
    @matrix(storage_mode=ALL_STORAGE_MODES)
    def test_producer_id_collision_after_failover(self, storage_mode):
        """Reproducer for CORE-16966: after a failover the promoted topic
        carries producer state inherited from the source, but the target's
        id_allocator is only advanced past the inherited pids by the link's
        asynchronous pid sync (maybe_sync_pid -> reset_next_id). Nothing
        orders that reset before the promoted topic starts serving
        init_producer_id; the very first reset also has to auto-create the
        target's id_allocator topic, which widens the window by seconds. If
        the first post-failover init_producer_id wins the race, the client
        gets a pid the source already used, its first batch matches the
        linked batch's (pid, seq) range, and rm_stm dedupes it and acks it
        at the linked records' offsets without appending anything -- silent
        write loss for clients that don't validate ack offsets.

        The race window is real but timing-bound: the test reproduces the
        failure on slow or loaded clusters (as in CI debug runs) and may
        pass on fast machines where the reset lands before the produce. The
        produce happens immediately after the failover completes to keep
        the window as tight as possible.
        """
        from confluent_kafka import Producer

        self.create_link("test-link")
        num_messages = 2
        topic = TopicSpec(name="test-topic", partition_count=1, replication_factor=3)
        self.create_source_topic(topic, storage_mode)

        def produce_idempotent(redpanda, start: int) -> list[int]:
            # A fresh idempotent producer: init_producer_id allocates a new
            # pid, and the records go out as a single batch with seq 0..1.
            producer = Producer(
                {
                    "bootstrap.servers": redpanda.brokers(),
                    "enable.idempotence": True,
                }
            )
            offsets: list[int] = []

            def on_delivery(err, msg):
                assert err is None, f"delivery failed: {err}"
                offsets.append(msg.offset())

            for i in range(start, start + num_messages):
                producer.produce(
                    topic.name,
                    value=f"msg-{i}".encode(),
                    partition=0,
                    on_delivery=on_delivery,
                )
            remaining = producer.flush(30)
            assert remaining == 0, f"{remaining} messages not delivered"
            return sorted(offsets)

        # The source producer takes the source cluster's first pid; the
        # link's sink replication carries its (pid, seq 0..1) producer state
        # to the target.
        offsets = produce_idempotent(self.source_cluster.service, 0)
        assert offsets == [0, 1], f"unexpected source offsets: {offsets}"

        self.target_cluster_service.wait_until(
            lambda: self.topic_exists_in_target(topic.name),
            timeout_sec=60,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        def target_consumed() -> bool:
            try:
                raw = self.target_cluster_rpk.consume(
                    topic=topic.name,
                    n=num_messages,
                    partition=0,
                    timeout=10,
                    format="%o,",
                )
                return len(raw.split(",")) - 1 == num_messages
            except Exception:
                return False

        # The linked records -- and with them the source producer's state --
        # must reach the target before the failover, else they are cut off.
        wait_until(
            target_consumed,
            timeout_sec=60,
            backoff_sec=1,
            err_msg="linked records never appeared in the target cluster",
        )

        self.failover_link(name="test-link")
        self.wait_for_link_failover(link="test-link")

        # A fresh producer against the promoted target, immediately after
        # failover. If the link's pid reset has not applied yet, the client
        # gets the same pid the source producer had; with the bug its batch
        # is deduped against the linked batch and acked at offsets [0, 1]
        # without being appended.
        offsets = produce_idempotent(self.target_cluster.service, num_messages)
        expected = [num_messages, num_messages + 1]
        assert offsets == expected, (
            f"post-failover records acked at offsets {offsets}, expected "
            f"{expected}: producer id collision, batch falsely deduped "
            "against producer state inherited through the link"
        )


class ShadowLinkUpdateBrokersTests(ShadowLinkPreAllocTestBase):
    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        self.test_context = test_context
        self.security = SecurityConfig()
        self.tls = TLSCertManager(self.logger)
        self.security.tls_provider = ClusterLinkingTLSProvider(self.tls)
        self.security.require_client_auth = False

        super().__init__(
            test_context=self.test_context, security=self.security, *args, **kwargs
        )

        self.other_source_cluster = RedpandaCluster.create(
            self.test_context,
            num_brokers=3,
            security=self.security,
        )

    def setUp(self):
        super().setUp()
        self.other_source_cluster.start()

    @property
    def target_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.target_cluster.service, tls_cert=self.tls.create_cert("target-rpk")
        )

    @property
    def other_source_cluster_rpk(self) -> RpkTool:
        return RpkTool(
            self.other_source_cluster.service,
            tls_cert=self.tls.create_cert("other-source-rpk"),
        )

    @cluster(
        num_nodes=9,
        log_allow_list=[re.compile(".*Broker.*does not support list groups API.*")],
    )
    def test_update_brokers(self):
        # Create a link pointing to the old source cluster
        shadow_link = self.create_link("test-link")

        # Update bootstrap_servers
        del shadow_link.configurations.client_options.bootstrap_servers[:]
        shadow_link.configurations.client_options.bootstrap_servers.extend(
            self.other_source_cluster.service.brokers_list()
        )

        # Update tls settings
        shadow_link.configurations.client_options.tls_settings.CopyFrom(
            tls_pb2.TLSSettings(
                enabled=True,
                tls_file_settings=tls_pb2.TLSFileSettings(
                    ca_path=self.redpanda.TLS_CA_CRT_FILE,
                    key_path=self.redpanda.TLS_SERVER_KEY_FILE,
                    cert_path=self.redpanda.TLS_SERVER_CRT_FILE,
                ),
            )
        )

        update_mask: google.protobuf.field_mask_pb2.FieldMask = (
            google.protobuf.field_mask_pb2.FieldMask(
                paths=["configurations.client_options"]
            )
        )

        # Update the link to point to the new source cluster
        updated_link = self.update_link(
            shadow_link=shadow_link, update_mask=update_mask
        )
        assert (
            updated_link.configurations.client_options
            == shadow_link.configurations.client_options
        ), (
            f"Expected updated link to be returned:\n"
            f"{updated_link.configurations.client_options}!=\n{shadow_link.configurations.client_options}"
        )

        old_source_topic = "old-source-topic"
        new_source_topic = "new-source-topic"

        self.source_cluster_rpk.create_topic(old_source_topic)
        self.other_source_cluster_rpk.create_topic(new_source_topic)

        self.target_cluster.service.wait_until(
            lambda: self.topic_exists_in_target(
                new_source_topic, 1, self.target_cluster_rpk
            ),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {new_source_topic} not found in target cluster",
        )
        assert not self.topic_exists_in_target(
            old_source_topic, None, self.target_cluster_rpk
        ), f"Topic {old_source_topic} should not be visible to the target cluster"


Validator = Callable[[list[dict[str, MetricSamples]]], bool]


class ShadowLinkingMetricsTests(ShadowLinkPreAllocTestBase):
    SHADOW_TOPIC_STATE = "redpanda_shadow_link_shadow_topic_state"
    TOTAL_RECORDS_FETCHED = "redpanda_shadow_link_total_records_fetched"
    TOTAL_RECORDS_WRITTEN = "redpanda_shadow_link_total_records_written"
    TOTAL_BYTES_FETCHED = "redpanda_shadow_link_total_bytes_fetched"
    TOTAL_BYTES_WRITTEN = "redpanda_shadow_link_total_bytes_written"
    SHADOW_LAG = "redpanda_shadow_link_shadow_lag"
    CLIENT_ERRORS = "redpanda_shadow_link_client_errors"

    def _get_metrics_for_node(
        self,
        node: ClusterNode,
        patterns: list[str],
    ) -> Optional[dict[str, MetricSamples]]:
        samples = self.redpanda.metrics_samples(
            patterns, [node], MetricsEndpoint.PUBLIC_METRICS
        )
        self.logger.debug(f"patterns: {patterns} node: {node.name} samples: {samples}")
        return samples

    def _get_metrics_for_nodes(
        self,
        nodes: list[ClusterNode],
        patterns: list[str],
    ) -> Optional[list[dict[str, MetricSamples]]]:
        metrics: list[dict[str, MetricSamples]] = []
        for n in nodes:
            node_metrics = self._get_metrics_for_node(n, patterns)

            if node_metrics is None:
                continue
            metrics.append(node_metrics)
        return metrics

    def _validate_metrics(
        self, nodes: list[ClusterNode], patterns: list[str], validator: Validator
    ):
        metrics = self._get_metrics_for_nodes(nodes, patterns)
        if metrics is None:
            return False
        return validator(metrics)

    @cluster(num_nodes=7)
    def test_link_metrics(self):
        topic_1 = TopicSpec(
            name="test-topic-1", partition_count=3, replication_factor=1
        )
        self.source_default_client().create_topic(topic_1)
        self.create_link("test-link")

        with self.producer_consumer(topic=topic_1.name, msg_size=128, msg_cnt=1000):
            self.verify()

        def collect_shadow_topic_states(
            node_samples: list[dict[str, MetricSamples]],
        ) -> dict[str, int]:
            by_status: dict[str, int] = {}
            for samples in node_samples:
                if self.SHADOW_TOPIC_STATE not in samples:
                    continue
                for s in samples[self.SHADOW_TOPIC_STATE].samples:
                    status = s.labels["status"]
                    if status not in by_status:
                        by_status[status] = 0
                    by_status[status] += int(s.value)
            return by_status

        def check_shadow_topic_states(
            node_samples: list[dict[str, MetricSamples]],
            expected_states: dict[str, int],
        ) -> bool:
            all_states = [
                "active",
                "failed",
                "paused",
                "failing_over",
                "failed_over",
                "promoting",
                "promoted",
            ]
            expected = {
                **expected_states,
                **{s: 0 for s in all_states if s not in expected_states},
            }

            by_status = collect_shadow_topic_states(node_samples)
            return all(by_status[s] == expected[s] for s in all_states)

        def _get_total_value(
            node_samples: list[dict[str, MetricSamples]], metric_name: str
        ) -> Optional[int]:
            total_value = 0
            for samples in node_samples:
                if metric_name not in samples:
                    return None
                for s in samples[metric_name].samples:
                    total_value += int(s.value)
            return total_value

        def check_total_value(
            node_samples: list[dict[str, MetricSamples]],
            metric_name: str,
            expected_total: int,
        ) -> bool:
            total_records = _get_total_value(node_samples, metric_name)
            return total_records is not None and total_records == expected_total

        # This function only checks that the result is greater than zero. i.e. something has been returned by this metric
        def check_value_positive(
            node_samples: list[dict[str, MetricSamples]], metric_name: str
        ) -> bool:
            total_value = _get_total_value(node_samples, metric_name)
            return total_value is not None and total_value > 0

        # This function checks that the result is at least min_value.
        def check_value_at_least(
            node_samples: list[dict[str, MetricSamples]],
            metric_name: str,
            min_value: int,
        ) -> bool:
            total_value = _get_total_value(node_samples, metric_name)
            return total_value is not None and total_value >= min_value

        def check_metric_exists(
            node_samples: list[dict[str, MetricSamples]], metric_name: str
        ) -> bool:
            for samples in node_samples:
                if metric_name not in samples:
                    return False
            return True

        def active_shadow_topics_1(samples: list[dict[str, MetricSamples]]):
            return check_shadow_topic_states(samples, {"active": 1})

        def active_shadow_topics_2(samples: list[dict[str, MetricSamples]]):
            return check_shadow_topic_states(samples, {"active": 2})

        def failed_over_topics_3(samples: list[dict[str, MetricSamples]]):
            return check_shadow_topic_states(samples, {"failed_over": 3})

        def check_records_fetched_1000(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_FETCHED, 1000)

        def check_records_fetched_2500(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_FETCHED, 2500)

        def check_records_written_1000(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_WRITTEN, 1000)

        def check_records_written_2500(samples: list[dict[str, MetricSamples]]):
            return check_total_value(samples, self.TOTAL_RECORDS_WRITTEN, 2500)

        def check_bytes_fetched(samples: list[dict[str, MetricSamples]]):
            return check_value_positive(samples, self.TOTAL_BYTES_FETCHED)

        def check_bytes_fetched_128000(samples: list[dict[str, MetricSamples]]):
            return check_value_at_least(samples, self.TOTAL_BYTES_FETCHED, 128000)

        def check_bytes_written(samples: list[dict[str, MetricSamples]]):
            return check_value_positive(samples, self.TOTAL_BYTES_WRITTEN)

        def check_bytes_written_128000(samples: list[dict[str, MetricSamples]]):
            return check_value_at_least(samples, self.TOTAL_BYTES_WRITTEN, 128000)

        def check_shadow_lag_zero(node_samples: list[dict[str, MetricSamples]]) -> bool:
            return check_total_value(node_samples, self.SHADOW_LAG, 0)

        def check_shadow_lag_positive(
            node_samples: list[dict[str, MetricSamples]],
        ) -> bool:
            return check_value_positive(node_samples, self.SHADOW_LAG)

        def check_client_errors(node_samples: list[dict[str, MetricSamples]]) -> bool:
            return check_metric_exists(node_samples, self.CLIENT_ERRORS)

        def validate_metrics(
            timeout_sec: int, metric_validators: list[tuple[str, Validator]]
        ):
            for metric_name, validator in metric_validators:
                self.logger.debug(
                    f"Validating values of metric: '{metric_name}', method: '{getattr(validator, '__name__')}'"
                )
                wait_until(
                    lambda: self._validate_metrics(
                        target_nodes, [metric_name], validator
                    ),
                    timeout_sec=timeout_sec,
                    backoff_sec=1,
                    err_msg=f"Failed to get the expected metrics value for metric {metric_name}",
                )

        target_nodes = self.target_cluster.service.nodes

        validate_metrics(
            timeout_sec=10,
            metric_validators=[
                (self.SHADOW_TOPIC_STATE, active_shadow_topics_1),
                (self.TOTAL_RECORDS_FETCHED, check_records_fetched_1000),
                (self.TOTAL_RECORDS_WRITTEN, check_records_written_1000),
                (self.TOTAL_BYTES_FETCHED, check_bytes_fetched_128000),
                (self.TOTAL_BYTES_WRITTEN, check_bytes_written_128000),
                (self.SHADOW_LAG, check_shadow_lag_zero),
                (self.CLIENT_ERRORS, check_client_errors),
            ],
        )

        topic_2 = TopicSpec(
            name="test-topic-2", partition_count=3, replication_factor=1
        )
        self.source_default_client().create_topic(topic_2)
        with self.producer_consumer(topic=topic_2.name, msg_size=128, msg_cnt=1500):
            self.verify()

        validate_metrics(
            timeout_sec=10,
            metric_validators=[
                (self.SHADOW_TOPIC_STATE, active_shadow_topics_2),
                (self.TOTAL_RECORDS_FETCHED, check_records_fetched_2500),
                (self.TOTAL_RECORDS_WRITTEN, check_records_written_2500),
                (self.TOTAL_BYTES_FETCHED, check_bytes_fetched),
                (self.TOTAL_BYTES_WRITTEN, check_bytes_written),
                (self.SHADOW_LAG, check_shadow_lag_zero),
                (self.CLIENT_ERRORS, check_client_errors),
            ],
        )

        topic_3 = TopicSpec(
            name="test-topic-3", partition_count=1, replication_factor=3
        )
        self.source_default_client().create_topic(topic_3)
        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic_3),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic_3.name} not found in target cluster",
        )

        with self.producer_consumer(
            topic=topic_3.name,
            msg_size=128,
            msg_cnt=5000000,
            use_transactions=True,
            producer_properties={
                "msgs_per_transaction": "100000",
            },
        ):
            validate_metrics(
                timeout_sec=120,
                metric_validators=[
                    (self.SHADOW_LAG, check_shadow_lag_positive),
                ],
            )
            self.verify()

        validate_metrics(
            timeout_sec=30,
            metric_validators=[
                (self.SHADOW_LAG, check_shadow_lag_zero),
            ],
        )

        self.failover_link(name="test-link")
        self.wait_for_link_failover(link="test-link")

        validate_metrics(
            timeout_sec=10,
            metric_validators=[
                (self.SHADOW_TOPIC_STATE, failed_over_topics_3),
            ],
        )


class ShadowLinkCustomStartOffsetSelectionTests(ShadowLinkPreAllocTestBase):
    earliest_offset = "earliest"
    latest_offset = "latest"
    timequery_offset = "timestamp"
    max_records = 10000

    def setup_starting_offset(
        self, topic: TopicSpec, storage_mode: str | None = None
    ) -> tuple[float, float]:
        initial = 1000
        assert self.max_records > initial
        self.create_source_topic(topic, storage_mode)
        start_time = time.time()
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic="source-topic",
            msg_size=4 * 1024,
            msg_count=initial,
            custom_node=self.preallocated_nodes,
        )
        # We produce in 2 phases so a batch cleanly ends at offset 999
        # and the next batch starts at offset 1000
        # This is done to workaround direct consumer limitation of not being
        # able to consume in the middle of a batch. If we don't do this here
        # the replication picks the first batch that contains the offset 1000
        # and hence the start offset may be before offset 1000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic="source-topic",
            msg_size=4 * 1024,
            msg_count=self.max_records - initial,
            custom_node=self.preallocated_nodes,
        )
        end_time = time.time()

        partitions = [
            p.id
            for p in self.source_cluster_rpk.describe_topic("source-topic", timeout=3)
        ]
        self.logger.info(f"Trimming source topic partitions: {partitions}")
        self.source_cluster_rpk.trim_prefix(
            topic="source-topic", offset=1000, partitions=partitions
        )

        def wait_for_starting_offset_1000(rpk: RpkTool):
            for part in rpk.describe_topic("source-topic"):
                if part.start_offset < 1000:
                    return False
            return True

        wait_until(
            lambda: wait_for_starting_offset_1000(self.source_cluster_rpk),
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Failed to trim source topic",
        )

        return (start_time, end_time)

    def find_starting_timestamp(
        self, topic: TopicSpec, start_time: float, end_time: float
    ) -> str:
        self.logger.info(
            f"Attempting to find a timestamp between {start_time} and {end_time}"
        )
        # Start at halfway point and go up 100ms at a time until we get an offset
        current_time = start_time + 1
        while current_time <= end_time:
            iso_timestamp = time.strftime(
                "%Y-%m-%dT%H:%M:%S", time.gmtime(current_time)
            )
            iso_formatted = f"{iso_timestamp}"
            self.logger.debug(f"Trying timestamp: {iso_formatted}")
            try:
                # Query each partition to see if we get a valid offset for this timestamp
                for part in self.source_cluster_rpk.describe_topic("source-topic"):
                    self.source_cluster_rpk.consume(
                        topic=topic.name,
                        n=1,
                        offset=f"@{iso_formatted}Z",
                        partition=part.id,
                        timeout=2,
                    )

                self.logger.info(f"Found starting offset: {iso_formatted}")
                return iso_formatted
            except Exception as e:
                self.logger.debug(f"Failed to query timestamp {iso_formatted}: {e}")

            current_time += 1  # Move forward 100ms

        # If no valid timestamp found, return the end time
        raise RuntimeError(
            f"Failed to find a valid starting timestamp between {start_time} and {end_time}"
        )

    @cluster(
        num_nodes=7,
        log_allow_list=CLOUD_TOPICS_SHADOW_LINK_LOG_ALLOW_LIST,
    )
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA, kafka_version="3.8.0", kafka_quorum="COMBINED_KRAFT"
            ),
        ],
        starting_offset=["earliest", "latest", "timestamp"],
        failures=[True, False],
        storage_mode=ALL_STORAGE_MODES,
    )
    def test_starting_offset(
        self,
        source_cluster_spec: SecondaryClusterSpec,
        starting_offset: str,
        failures: bool,
        storage_mode: str,
    ):
        """
        This test will verify the starting offset configuration.

        1. Pre-populate the source cluster with some data
        2. Prefix-truncate the data
        3. Create a shadow link with a specified starting offset
        4. Verify that the shadow topic is starting at the specified starting offset
        """
        if not self.source_cluster.is_redpanda:
            storage_mode = TopicSpec.STORAGE_MODE_LOCAL

        if failures and (
            source_cluster_spec.cluster_type == ServiceType.KAFKA
            or starting_offset == "latest"
        ):
            # 1. Kafka source does not support transient failures injection
            # 2. With failures enabled, in latest offset mode, it is hard to guarantee
            # the replicator will pick the exact latest offset due to retries
            # and back-offs (since latest is a moving target).
            # Avoid warning of not using all allocated nodes
            _ = self.preallocated_nodes
            self.logger.info("Skipping failure injection with Kafka source cluster")
            return

        if (
            starting_offset == self.timequery_offset
            and storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2
        ):
            # Timestamp queries on tiered_cloud shadow topics are not
            # yet supported.
            _ = self.preallocated_nodes
            self.logger.info("Skipping timestamp starting offset for tiered_cloud")
            return
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)

        (start_time, end_time) = self.setup_starting_offset(
            topic=topic, storage_mode=storage_mode
        )

        req = self.create_default_link_request("test-link")

        if starting_offset == self.earliest_offset:
            req.shadow_link.configurations.topic_metadata_sync_options.start_at_earliest.CopyFrom(
                shadow_link_pb2.TopicMetadataSyncOptions.EarliestOffset()
            )
        elif starting_offset == self.latest_offset:
            req.shadow_link.configurations.topic_metadata_sync_options.start_at_latest.CopyFrom(
                shadow_link_pb2.TopicMetadataSyncOptions.LatestOffset()
            )
        elif starting_offset == self.timequery_offset:
            starting_offset = self.find_starting_timestamp(
                topic=topic, start_time=start_time, end_time=end_time
            )
            self.logger.info(f'Using starting offset "{starting_offset}"')
            timestamp_pb = google.protobuf.timestamp_pb2.Timestamp()
            timestamp_pb.FromMilliseconds(
                int(
                    time.mktime(time.strptime(starting_offset, "%Y-%m-%dT%H:%M:%S"))
                    * 1000
                )
            )
            req.shadow_link.configurations.topic_metadata_sync_options.start_at_timestamp.CopyFrom(
                timestamp_pb
            )
        else:
            assert False, f"Invalid starting offset value: {starting_offset}"

        def maybe_failure_injector():
            if failures:
                # Inject failures on source to simulate transient errors during
                # timestamp to offset resolution and subsequent retries.
                return self.create_source_failure_injector()
            else:
                return self._nop_context_manager()

        with maybe_failure_injector():
            self.create_link_with_request(req=req)

            self.target_cluster.service.wait_until(
                lambda: self.topic_partitions_exists_in_target(topic),
                timeout_sec=30,
                backoff_sec=1,
                err_msg=f"Topic {topic.name} not found in target cluster",
            )

            if starting_offset == self.latest_offset:
                # link should start now at the latest offset and should be empty
                def ensure_target_partition_empty():
                    try:
                        for part in self.target_cluster_rpk.describe_topic(topic.name):
                            if part.high_watermark != part.start_offset:
                                return False
                        return True
                    except Exception as e:
                        self.logger.debug(f"Failed to describe topic: {e}")
                        return False

                wait_until(
                    ensure_target_partition_empty,
                    timeout_sec=60,
                    backoff_sec=1,
                    err_msg=f"Target topic {topic.name} partitions not empty",
                )

                def produce_one_key():
                    try:
                        self.source_cluster_rpk.produce(
                            topic=topic.name, key="key", msg="value", partition=0
                        )
                        return True
                    except Exception as e:
                        self.logger.debug(f"Failed to produce to topic: {e}")
                        return False

                wait_until(
                    produce_one_key,
                    timeout_sec=30,
                    backoff_sec=1,
                    err_msg=f"Failed to produce to source topic {topic.name}",
                )

            def get_partitions_starting_offset(
                rpk: RpkTool, offset: str | int
            ) -> dict[int, int]:
                def do_get_partitions_starting_offset():
                    try:
                        offsets: dict[int, int] = {}
                        for part in rpk.describe_topic(topic.name, timeout=3):
                            record = json.loads(
                                rpk.consume(
                                    topic=topic.name,
                                    n=1,
                                    partition=part.id,
                                    offset=offset,
                                )
                            )
                            offsets[part.id] = record["offset"]

                        return offsets
                    except Exception as e:
                        self.logger.debug(
                            f"Failed to get partitions starting offsets: {e}"
                        )
                        return None

                return wait_until_result(
                    do_get_partitions_starting_offset,
                    timeout_sec=60,
                    backoff_sec=1,
                    err_msg=f"Failed to get partitions starting offsets for {topic.name}",
                )

            source_offset_to_fetch = (
                "start"
                if starting_offset == self.earliest_offset
                else "-1"
                if starting_offset == self.latest_offset
                else f"@{starting_offset}Z"
            )
            self.logger.info(
                f"Fetching offset '{source_offset_to_fetch}' from source cluster"
            )
            source_offsets = get_partitions_starting_offset(
                self.source_cluster_rpk, source_offset_to_fetch
            )
            self.logger.info(f"Source cluster offsets: {source_offsets}")

            def do_wait_for_hwm():
                partition_info = list(
                    self.target_cluster_rpk.describe_topic(topic.name)
                )
                for p in partition_info:
                    if p.id == 0:
                        return p.high_watermark >= self.max_records

            self.target_cluster_service.wait_until(
                do_wait_for_hwm,
                timeout_sec=60,
                backoff_sec=2,
                err_msg="Timed out waiting for hwm to catchup on target",
                retry_on_exc=True,
            )

            # If testing for earliest or latest offset, use "start", else
            # use the timequery value.  The batch fetched from the source at
            # that time query may have records starting before that timestamp
            target_offset_to_fetch = (
                "start"
                if starting_offset == self.earliest_offset
                or starting_offset == self.latest_offset
                else f"@{starting_offset}Z"
            )
            self.logger.info(
                f"Fetching offset '{target_offset_to_fetch}' from target cluster"
            )
            target_offsets = get_partitions_starting_offset(
                self.target_cluster_rpk, target_offset_to_fetch
            )
            self.logger.info(f"Target cluster starting offsets: {target_offsets}")

            assert source_offsets == target_offsets, (
                f"Expected source and target offsets to match, got {target_offsets} vs {source_offsets}"
            )

    @cluster(num_nodes=7)
    @matrix(
        source_cluster_spec=[
            SecondaryClusterSpec(ServiceType.REDPANDA),
            SecondaryClusterSpec(
                ServiceType.KAFKA,
                kafka_version="3.8.0",
                kafka_quorum="COMBINED_KRAFT",
            ),
        ],
    )
    def test_start_at_future_timestamp(
        self,
        source_cluster_spec: SecondaryClusterSpec,
    ):
        """
        Verify that when a shadow link is configured with a start timestamp
        past the end of the source log, replication begins at the LSO rather
        than offset 0.

        ListOffsets returns offset -1 with error_code=none when the requested
        timestamp exceeds all data in the partition. The fix detects this and
        falls back to the LSO so only new data is replicated.
        """
        _ = source_cluster_spec
        topic = TopicSpec(name="source-topic", partition_count=1, replication_factor=3)
        self.source_default_client().create_topic(topic)

        # Produce historical data that must NOT be replicated to the target.
        initial_msg_count = 1000
        KgoVerifierProducer.oneshot(
            self.test_context,
            self.source_cluster.service,
            topic=topic.name,
            msg_size=4 * 1024,
            msg_count=initial_msg_count,
            custom_node=self.preallocated_nodes,
        )

        def get_partition_0_info(rpk: RpkTool) -> RpkPartition | None:
            try:
                for part in rpk.describe_topic(topic.name, timeout=3):
                    if part.id == 0:
                        return part
            except Exception as e:
                self.logger.debug(f"Failed to describe topic: {e}")
            return None

        def source_hwm_reached_msg_count() -> int | None:
            p_info = get_partition_0_info(self.source_cluster_rpk)
            if p_info and p_info.high_watermark >= initial_msg_count:
                return p_info.high_watermark
            return None

        source_orig_hwm = wait_until_result(
            source_hwm_reached_msg_count,
            timeout_sec=30,
            backoff_sec=1,
            err_msg="Timed out waiting for source HWM to reach expected count",
        )
        self.logger.info(f"Source HWM before link creation: {source_orig_hwm}")

        # Configure the link with a timestamp far in the future so that
        # ListOffsets returns offset -1 (no record at or after that timestamp).
        req = self.create_default_link_request("test-link")
        timestamp_pb = google.protobuf.timestamp_pb2.Timestamp()
        timestamp_pb.FromMilliseconds(
            int(
                time.mktime(time.strptime("2100-01-01T00:00:00", "%Y-%m-%dT%H:%M:%S"))
                * 1000
            )
        )
        req.shadow_link.configurations.topic_metadata_sync_options.start_at_timestamp.CopyFrom(
            timestamp_pb
        )
        self.create_link_with_request(req=req)

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        # Confirm that the link is not replicating historical data (both HWM and start offset stay 0 on the target).
        sleep(5)
        prev_target_info = get_partition_0_info(self.target_cluster_rpk)
        assert prev_target_info is not None, "Failed to get target partition info"
        assert prev_target_info.high_watermark == 0, (
            f"Expected target HWM to be 0, got {prev_target_info.high_watermark}"
        )
        assert prev_target_info.start_offset == 0, (
            f"Expected target start offset to be 0, got {prev_target_info.start_offset}"
        )

        self.logger.info("Producing one new record to the source topic")
        self.source_cluster_rpk.produce(
            topic=topic.name, key="key", msg="value", partition=0
        )

        source_info = get_partition_0_info(self.source_cluster_rpk)
        assert source_info is not None, (
            "Failed to get source partition info after producing new record"
        )
        assert source_info.high_watermark == source_orig_hwm + 1, (
            f"Expected source HWM to advance by 1 after producing new record, "
            f"got {source_info.high_watermark} vs previous {source_orig_hwm}"
        )

        def target_has_new_data():
            target_info = get_partition_0_info(self.target_cluster_rpk)
            if target_info is None:
                return False

            return (
                target_info.high_watermark == source_info.high_watermark,
                target_info,
            )

        target_info: RpkPartition = wait_until_result(
            target_has_new_data,
            timeout_sec=60,
            backoff_sec=2,
            err_msg="New data was not replicated to the target cluster",
            retry_on_exc=True,
        )

        assert target_info.high_watermark == source_info.high_watermark, (
            f"Expected target HWM to be {source_info.high_watermark} after replicating new record, got {target_info.high_watermark}"
        )

        source_last_record = json.loads(
            self.source_cluster_rpk.consume(
                topic=topic.name, n=1, partition=0, offset=-1
            )
        )
        target_first_record = json.loads(
            self.target_cluster_rpk.consume(
                topic=topic.name, n=1, partition=0, offset="start"
            )
        )
        assert source_last_record == target_first_record, (
            f"Record mismatch: source={source_last_record}, target={target_first_record}"
        )


class ShadowLinkingCloudTopicReplicationTests(ShadowLinkPreAllocTestBase):
    """
    Tests cluster linking replication with cloud topics
    (redpanda.storage.mode=cloud and tiered with version tiered_v2) on the
    source cluster.
    """

    def __init__(self, test_context: TestContext, *args: Any, **kwargs: Any):
        si_settings = SISettings(
            test_context,
            cloud_storage_max_connections=10,
            cloud_storage_enable_remote_read=False,
            cloud_storage_enable_remote_write=False,
            fast_uploads=True,
        )

        super().__init__(
            test_context,
            si_settings=si_settings,
            extra_rp_conf={
                "enable_cluster_metadata_upload_loop": False,
            },
            secondary_cluster_args=SecondaryClusterArgs(
                si_settings=si_settings,
                extra_rp_conf={
                    "enable_shadow_linking": True,
                    "enable_cluster_metadata_upload_loop": False,
                },
            ),
            *args,
            **kwargs,
        )

    @cluster(num_nodes=7)
    @matrix(
        storage_mode=[
            TopicSpec.STORAGE_MODE_CLOUD,
            TopicSpec.STORAGE_MODE_IMPL_TIERED_V2,
        ],
    )
    def test_cloud_topic_replication(self, storage_mode):
        """
        Verify that data produced to a cloud/tiered_v2 topic on the source
        cluster is replicated to the target cluster via cluster linking.
        """
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            self.source_cluster_service.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )
            self.target_cluster.service.set_feature_active(
                "tiered_cloud_topics", True, timeout_sec=30
            )

        topic = TopicSpec(
            name="ct-topic",
            partition_count=3,
            replication_factor=1,
        )

        source_rpk = RpkTool(self.source_cluster.service)

        def create_source_topic():
            try:
                source_rpk.create_topic(
                    topic=topic.name,
                    partitions=topic.partition_count,
                    replicas=topic.replication_factor,
                    config=TopicSpec.storage_mode_config(storage_mode),
                )
                return True
            except Exception as e:
                if "INVALID_CONFIG" in str(e):
                    return False
                raise

        # Retry topic creation: feature flag propagation may lag behind
        # the admin API response on some nodes.
        wait_until(
            create_source_topic,
            timeout_sec=30,
            backoff_sec=2,
            err_msg=f"Failed to create source topic with storage_mode={storage_mode}",
        )

        expected_mode = (
            TopicSpec.STORAGE_MODE_TIERED
            if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2
            else storage_mode
        )

        source_configs = source_rpk.describe_topic_configs(topic.name)
        assert source_configs[TopicSpec.PROPERTY_STORAGE_MODE][0] == expected_mode, (
            f"Source topic storage mode: {source_configs[TopicSpec.PROPERTY_STORAGE_MODE]}, "
            f"expected: {expected_mode}"
        )
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            source_version = source_configs[TopicSpec.PROPERTY_STORAGE_MODE_IMPL][0]
            assert source_version == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2, (
                f"Source topic storage mode version: {source_version}"
            )

        self.create_link("test-link")

        self.target_cluster.service.wait_until(
            lambda: self.topic_partitions_exists_in_target(topic),
            timeout_sec=30,
            backoff_sec=1,
            err_msg=f"Topic {topic.name} not found in target cluster",
        )

        # Verify target topic has the same storage mode
        target_rpk = RpkTool(self.target_cluster.service)
        target_configs = target_rpk.describe_topic_configs(topic.name)
        assert target_configs[TopicSpec.PROPERTY_STORAGE_MODE][0] == expected_mode, (
            f"Target topic storage mode: {target_configs[TopicSpec.PROPERTY_STORAGE_MODE]}, "
            f"expected: {expected_mode}"
        )
        if storage_mode == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2:
            target_version = target_configs[TopicSpec.PROPERTY_STORAGE_MODE_IMPL][0]
            assert target_version == TopicSpec.STORAGE_MODE_IMPL_TIERED_V2, (
                f"Target topic storage mode version: {target_version}"
            )

        with self.producer_consumer(topic=topic.name, msg_size=128, msg_cnt=10000):
            self.verify()
