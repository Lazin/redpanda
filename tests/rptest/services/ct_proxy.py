# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import tempfile
from threading import Event
from typing import Optional

import requests
import yaml
from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.background_thread import BackgroundThreadService


class CtProxyService(BackgroundThreadService):
    """
    Service that runs ct-proxy alongside Redpanda.

    ct-proxy is a Go application that proxies Kafka protocol requests to
    Redpanda's cloud topics, handling L0 object uploads and placeholder
    replication.
    """

    # Default ports for ct-proxy
    DEFAULT_KAFKA_PORT = 19092
    DEFAULT_ADMIN_PORT = 19644

    # Log file path on remote nodes
    LOG_FILE = "/tmp/ct-proxy.log"
    CONFIG_FILE = "/tmp/ct-proxy-config.yaml"

    # Ducktape log collection - logs are copied to test output directory
    logs = {
        "ct_proxy_log": {
            "path": LOG_FILE,
            "collect_default": True,
        },
        "ct_proxy_config": {
            "path": CONFIG_FILE,
            "collect_default": True,
        },
    }

    def __init__(
        self,
        context,
        redpanda,
        topic: str,
        *,
        kafka_port: int = DEFAULT_KAFKA_PORT,
        admin_port: int = DEFAULT_ADMIN_PORT,
        cloud_storage_bucket: str = "test-bucket",
        cloud_storage_region: str = "us-east-1",
        cloud_storage_provider: str = "aws",
        log_level: str = "info",
    ):
        """
        Initialize the ct-proxy service.

        :param context: The test context
        :param redpanda: The RedpandaService instance
        :param topic: The cloud topic name to allow
        :param kafka_port: Port for Kafka protocol listener
        :param admin_port: Port for HTTP admin API
        :param cloud_storage_bucket: S3 bucket name
        :param cloud_storage_region: S3 region
        :param cloud_storage_provider: Cloud provider (aws, gcp, azure)
        :param log_level: Log level (debug, info, warn, error)
        """
        self._redpanda = redpanda
        self._topic = topic
        self._kafka_port = kafka_port
        self._admin_port = admin_port
        self._cloud_storage_bucket = cloud_storage_bucket
        self._cloud_storage_region = cloud_storage_region
        self._cloud_storage_provider = cloud_storage_provider
        self._log_level = log_level
        self._stopping = Event()
        self._node = None
        self._admin_address = None
        super(CtProxyService, self).__init__(context, num_nodes=1)

    def _generate_config(self, node) -> dict:
        """Generate ct-proxy configuration."""
        # Get admin API addresses from started Redpanda nodes
        # The gRPC admin API (ConnectRPC) runs on the same port as HTTP admin API (9644)
        admin_addresses = self._redpanda.admin_endpoints_list()

        self._redpanda.logger.info(
            f"ct-proxy will connect to Redpanda admin API at: {admin_addresses}"
        )

        return {
            "server": {
                "kafka_listen_address": f"0.0.0.0:{self._kafka_port}",
                "admin_listen_address": f"0.0.0.0:{self._admin_port}",
            },
            "redpanda": {
                "admin_api": {
                    "addresses": admin_addresses,
                    "tls": {
                        "enabled": False,
                    },
                },
            },
            "cloud_storage": {
                "provider": self._cloud_storage_provider,
                "region": self._cloud_storage_region,
                "bucket": self._cloud_storage_bucket,
            },
            "cloud_topics": {
                "allowed_topics": [self._topic],
            },
            "logging": {
                "level": self._log_level,
                "format": "text",
            },
        }

    def _worker(self, idx, node):
        """Run ct-proxy on the node."""
        self._node = node
        self._stopping.clear()

        # Set admin address early so wait_ready can use it
        self._admin_address = f"{node.account.hostname}:{self._admin_port}"

        self._redpanda.logger.info(
            f"ct-proxy worker started on {node.account.hostname}, "
            f"admin API will be at {self._admin_address}"
        )

        # Find the ct-proxy binary
        try:
            binary_path = self._find_ct_proxy_binary(node)
        except FileNotFoundError as e:
            self._redpanda.logger.error(f"ct-proxy binary not found: {e}")
            # Write error to log file for debugging
            node.account.ssh(
                f"echo 'ERROR: ct-proxy binary not found' > {self.LOG_FILE}"
            )
            raise

        # Generate and write config file
        config = self._generate_config(node)
        config_yaml = yaml.dump(config)

        # Write config to a temp file on the node
        node.account.create_file(self.CONFIG_FILE, config_yaml)

        self._redpanda.logger.info(
            f"Starting ct-proxy on {node.account.hostname} "
            f"with binary: {binary_path}, config: {config}"
        )

        # Run ct-proxy and redirect output to log file
        cmd = f"{binary_path} --config {self.CONFIG_FILE} 2>&1 | tee {self.LOG_FILE}"

        try:
            for line in node.account.ssh_capture(cmd, combine_stderr=True):
                self._redpanda.logger.info(f"ct-proxy: {line.rstrip()}")
                if self._stopping.is_set():
                    break
        except RemoteCommandError as e:
            if self._stopping.is_set():
                pass
            else:
                # Log the error and any output from the log file
                self._redpanda.logger.error(f"ct-proxy failed: {e}")
                try:
                    log_output = node.account.ssh_output(
                        f"cat {self.LOG_FILE} 2>/dev/null || echo 'No log file'"
                    )
                    self._redpanda.logger.error(f"ct-proxy log: {log_output}")
                except Exception:
                    pass
                raise

    def _find_ct_proxy_binary(self, node) -> str:
        """Find the ct-proxy binary path on the given node."""
        # In ducktape tests, binaries are typically in a known location
        # Check common locations
        candidates = []

        rp_install_path_root = self.context.globals.get(
            "rp_install_path_root", None
        )
        if rp_install_path_root:
            candidates.append(f"{rp_install_path_root}/bin/ct-proxy")

        # Add fallback locations
        candidates.extend([
            "/opt/redpanda/bin/ct-proxy",
            "/usr/bin/ct-proxy",
            "/usr/local/bin/ct-proxy",
        ])

        # Check which binary exists on the node
        for path in candidates:
            try:
                node.account.ssh(f"test -x {path}", allow_fail=False)
                self._redpanda.logger.info(f"Found ct-proxy binary at {path}")
                return path
            except Exception:
                self._redpanda.logger.debug(f"ct-proxy not found at {path}")
                continue

        # If no binary found, raise an error with helpful message
        raise FileNotFoundError(
            f"ct-proxy binary not found on {node.account.hostname}. "
            f"Checked paths: {candidates}. "
            f"Make sure ct-proxy is installed or set rp_install_path_root."
        )

    def stop_node(self, node):
        """Stop ct-proxy on the node."""
        self._stopping.set()
        try:
            node.account.kill_process("ct-proxy", clean_shutdown=False)
        except Exception:
            pass

    def clean_node(self, node):
        """Clean up ct-proxy artifacts on the node."""
        for f in [self.CONFIG_FILE, self.LOG_FILE]:
            try:
                node.account.remove(f, allow_fail=True)
            except Exception:
                pass

    @property
    def admin_address(self) -> str | None:
        """Get the admin API address."""
        return self._admin_address

    def admin_url(self) -> str | None:
        """Get the admin API URL."""
        if self._admin_address:
            return f"http://{self._admin_address}"
        return None

    def get_epoch(self, topic: str, partition: int = 0, timeout: float = 10.0) -> int:
        """
        Get the cluster epoch for a topic partition via the HTTP API.

        :param topic: The topic name
        :param partition: The partition number
        :param timeout: Request timeout in seconds
        :return: The cluster epoch
        :raises: requests.RequestException on failure
        """
        if not self._admin_address:
            raise RuntimeError("ct-proxy is not running")

        url = f"http://{self._admin_address}/api/epoch"
        params = {"topic": topic, "partition": str(partition)}

        response = requests.get(url, params=params, timeout=timeout)
        response.raise_for_status()

        data = response.json()
        return data["cluster_epoch"]

    def list_topics(self, timeout: float = 10.0) -> list[str]:
        """
        List topics via the HTTP API.

        :param timeout: Request timeout in seconds
        :return: List of topic names
        :raises: requests.RequestException on failure
        """
        if not self._admin_address:
            raise RuntimeError("ct-proxy is not running")

        url = f"http://{self._admin_address}/api/topics"

        response = requests.get(url, timeout=timeout)
        response.raise_for_status()

        data = response.json()
        return data.get("topics", [])

    def health_check(self, timeout: float = 5.0) -> bool:
        """
        Check if ct-proxy is healthy via the health endpoint.

        :param timeout: Request timeout in seconds
        :return: True if healthy, False otherwise
        """
        if not self._admin_address:
            return False

        try:
            url = f"http://{self._admin_address}/api/health"
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return data.get("status") == "ok"
        except Exception:
            return False

    def wait_ready(self, timeout_sec: float = 30.0):
        """
        Wait for ct-proxy to be ready by checking the health endpoint.

        :param timeout_sec: Maximum time to wait in seconds
        :raises: TimeoutError if ct-proxy doesn't become ready
        """
        from ducktape.utils.util import wait_until

        def is_ready():
            ready = self.health_check(timeout=5.0)
            if not ready:
                self._redpanda.logger.debug(
                    f"ct-proxy health check failed, admin_address={self._admin_address}"
                )
            return ready

        def err_msg():
            msg = "ct-proxy did not become ready"
            if self._node:
                try:
                    log_output = self._node.account.ssh_output(
                        f"tail -50 {self.LOG_FILE} 2>/dev/null || echo 'No log'"
                    )
                    msg += f"\nct-proxy log:\n{log_output}"
                except Exception:
                    pass
            return msg

        wait_until(
            is_ready,
            timeout_sec=timeout_sec,
            backoff_sec=1,
            err_msg=err_msg,
        )

    # Methods to support RpkTool connection to ct-proxy
    # These methods mirror the RedpandaService interface so RpkTool can connect

    @property
    def logger(self):
        """Return the logger (delegated to redpanda service)."""
        return self._redpanda.logger

    @property
    def _context(self):
        """Return the test context (required by RpkTool to find rpk binary)."""
        return self.context

    def brokers(self) -> str:
        """
        Get the ct-proxy Kafka broker address.
        This allows RpkTool to connect to ct-proxy instead of Redpanda.

        :return: Comma-separated broker addresses (single address for ct-proxy)
        """
        if not self._node:
            raise RuntimeError("ct-proxy is not running")
        return f"{self._node.account.hostname}:{self._kafka_port}"

    def brokers_list(self) -> list[str]:
        """
        Get the ct-proxy Kafka broker address as a list.

        :return: List of broker addresses
        """
        return [self.brokers()]

    def kafka_client_security(self):
        """
        Get the Kafka client security settings for ct-proxy.
        Currently ct-proxy does not support authentication.

        :return: Security settings (no authentication)
        """
        from rptest.services.redpanda_types import PLAINTEXT_SECURITY
        return PLAINTEXT_SECURITY
