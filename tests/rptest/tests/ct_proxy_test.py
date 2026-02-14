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
                "redpanda.cloud_topic.enabled": "true",
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
