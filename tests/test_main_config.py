from __future__ import annotations

from peetsfea_runner.main import ACCOUNTS, CONFIG


def test_single_gate_account_configuration_is_fixed() -> None:
    assert len(ACCOUNTS) == 1
    account = ACCOUNTS[0]
    assert account.account_id == "harry261"
    assert account.ssh_alias == "gate1-harry"
    assert account.spool_paths.inbox == "/home1/harry261/peetsfea-spool/inbox"
    assert account.spool_paths.claimed == "/home1/harry261/peetsfea-spool/claimed"
    assert account.spool_paths.results == "/home1/harry261/peetsfea-spool/results"
    assert account.spool_paths.failed == "/home1/harry261/peetsfea-spool/failed"



def test_worker_pool_target_is_10() -> None:
    assert CONFIG.slurm_policy.pool_target_per_account == 10
