from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from peetsfea_runner.pipeline import PipelineConfig, build_lease_server_context


def test_build_lease_server_context_includes_default_hfss_license_query_fields() -> None:
    config = PipelineConfig(input_queue_dir="./input_queue")

    context = build_lease_server_context(config=config)

    assert context.ssh_config_path == config.ssh_config_path
    assert context.hfss_license_gate_enabled is True
    assert context.hfss_license_source_host == "gate1-harry261"
    assert context.hfss_license_feature_name == "elec_solve_hfss"
    assert context.hfss_license_ceiling == 530
    assert context.hfss_license_cache_ttl_seconds == 10
    assert context.hfss_license_query_timeout_seconds == 30
    assert context.hfss_license_poll_env == "ANSYSLMD_LICENSE_FILE=1055@172.16.10.81"
    assert (
        context.hfss_license_poll_command
        == "/opt/ohpc/pub/Electronics/v252/licensingclient/linx64/lmutil lmstat -a"
    )


def test_build_lease_server_context_preserves_hfss_license_query_overrides() -> None:
    with TemporaryDirectory() as tmpdir:
        ssh_config = Path(tmpdir) / "ssh-config"
        ssh_config.write_text("Host license-host\n", encoding="utf-8")
        config = PipelineConfig(
            input_queue_dir="./input_queue",
            ssh_config_path=str(ssh_config),
            hfss_license_gate_enabled=False,
            hfss_license_source_host="license-host",
            hfss_license_feature_name="custom_hfss",
            hfss_license_ceiling=512,
            hfss_license_cache_ttl_seconds=7,
            hfss_license_query_timeout_seconds=9,
            hfss_license_poll_env="ENV=value",
            hfss_license_poll_command="/bin/lmstat -a",
        )

        context = build_lease_server_context(config=config)

    assert context.ssh_config_path == str(ssh_config)
    assert context.hfss_license_gate_enabled is False
    assert context.hfss_license_source_host == "license-host"
    assert context.hfss_license_feature_name == "custom_hfss"
    assert context.hfss_license_ceiling == 512
    assert context.hfss_license_cache_ttl_seconds == 7
    assert context.hfss_license_query_timeout_seconds == 9
    assert context.hfss_license_poll_env == "ENV=value"
    assert context.hfss_license_poll_command == "/bin/lmstat -a"
