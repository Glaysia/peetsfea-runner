from __future__ import annotations

from peetsfea_runner.edt_container_control import ContainerController


def test_init_starts_at_target_clamped() -> None:
    # 적분 상태는 지령으로 초기화(범위 클램프).
    assert ContainerController(target_aedt=120, n_min=80, n_max=150).n_total == 120
    assert ContainerController(target_aedt=200, n_min=80, n_max=150).n_total == 150  # 상한 클램프


def test_tick_step_direction_and_clamp() -> None:
    c = ContainerController(target_aedt=120, ki=0.4, dn_max=3, n_min=0, n_max=1000, n_total=100)
    # 관측<지령 → err>0 → N 증가, 단 ±dn_max로 클램프(0.4*100=40 → +3).
    assert c.tick(20) == 103
    # 관측>지령 → N 감소, 클램프(-3).
    assert c.tick(200) == 100
    # 관측=지령 → 변화 0(정상상태).
    assert c.tick(120) == 100


def test_integral_converges_to_zero_offset() -> None:
    # 적분: 작은 정상 오차가 계속 있으면 N이 한계까지 누적된다(비례항과 달리 옵셋을 0으로).
    c = ContainerController(target_aedt=120, ki=0.4, dn_max=3, n_min=80, n_max=150, n_total=100)
    for _ in range(100):
        c.tick(118)  # 항상 2 부족 → 계속 +1씩(round(0.4*2)=1) 누적
    assert c.n_total == 150  # 상한까지 적분 누적(옵셋 해소 방향)


def test_distribute_sums_and_balances() -> None:
    c = ContainerController(job_count=10, n_total=124)
    dist = c.distribute()
    assert sum(dist) == 124            # 합 보존
    assert max(dist) - min(dist) <= 1  # 균등(±1)
    assert dist == [13, 13, 13, 13, 12, 12, 12, 12, 12, 12]  # 나머지 4개를 앞에서부터


def test_plan_for_matches_distribution() -> None:
    c = ContainerController(job_count=10, n_total=124)
    assert c.plan_for(0) == 13 and c.plan_for(9) == 12
    assert c.plan_for(99) == 0  # 범위 밖


def test_n_total_respects_bounds_across_ticks() -> None:
    c = ContainerController(target_aedt=120, ki=0.5, dn_max=3, n_min=90, n_max=130, n_total=130)
    for _ in range(50):
        c.tick(0)  # 강하게 증가 압력이어도 상한 고정
    assert c.n_total == 130
    for _ in range(50):
        c.tick(1000)  # 강한 감소 압력이어도 하한 고정
    assert c.n_total == 90
