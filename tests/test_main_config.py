from __future__ import annotations

from peetsfea_runner.main import ACCOUNTS, CONFIG


def test_gate_accounts_configuration_is_fixed() -> None:
    assert len(ACCOUNTS) == 4
    aliases = tuple(account.ssh_alias for account in ACCOUNTS)
    assert aliases == ("gate1-harry", "gate1-hmlee31", "gate1-dhj02", "gate1-wjddn5916")
    assert ACCOUNTS[0].spool_paths.inbox == "/gpfs/home1/harry261/peetsfea-spool/inbox"
    assert ACCOUNTS[1].spool_paths.inbox == "/gpfs/home1/hmlee31/peetsfea-spool/inbox"
    assert ACCOUNTS[2].spool_paths.inbox == "/gpfs/home1/dhj02/peetsfea-spool/inbox"
    assert ACCOUNTS[3].spool_paths.inbox == "/gpfs/home1/wjddn5916/peetsfea-spool/inbox"



def test_worker_pool_target_is_10() -> None:
    assert CONFIG.slurm_policy.pool_target_per_account == 10
    assert CONFIG.slurm_policy.job_internal_procs == 8
    assert CONFIG.slurm_policy.cores == 32
    assert CONFIG.slurm_policy.memory_gb == 320
    assert CONFIG.slurm_policy.repo_url == "https://github.com/Glaysia/peetsfea-runner"
    assert CONFIG.slurm_policy.release_tag == "v2026.03.02-gate1-r1"
