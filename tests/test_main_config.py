from __future__ import annotations

from peetsfea_runner.main import ACCOUNTS, CONFIG


def test_single_gate_account_configuration_is_fixed() -> None:
    assert len(ACCOUNTS) == 1
    account = ACCOUNTS[0]
    assert account.account_id == "win5600x2"
    assert account.ssh_alias == "5600X2"
    assert account.spool_paths.inbox == "C:/peetsfea-spool/inbox"
    assert account.spool_paths.claimed == "C:/peetsfea-spool/claimed"
    assert account.spool_paths.results == "C:/peetsfea-spool/results"
    assert account.spool_paths.failed == "C:/peetsfea-spool/failed"



def test_worker_pool_target_is_10() -> None:
    assert CONFIG.slurm_policy.pool_target_per_account == 1
    assert CONFIG.slurm_policy.repo_url == "https://github.com/Glaysia/peetsfea-runner"
    assert CONFIG.slurm_policy.release_tag == "v2026.03.02-5600x2-r1"
