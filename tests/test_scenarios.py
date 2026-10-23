"""System-level scenarios against the inspect cluster, each in its own on-demand environment."""
import time

import pytest

from fleetlab import HarnessError

pytestmark = pytest.mark.docker
ALL_NODES = [f"node-{i}" for i in range(1, 6)]


def test_baseline_every_job_completes_exactly_once(cluster, record):
    cluster.submit(60)
    st = cluster.wait_done(60)
    cluster.assert_exactly_once(st, 60)
    assert st["node_deaths"] == 0 and st["redispatches"] == 0, st
    record.update(st)


def test_network_delay_and_jitter_cause_no_false_failures(cluster, record):
    for n in ALL_NODES:
        cluster.c.netem(n, delay_ms=300, jitter_ms=50)
    cluster.submit(40)
    st = cluster.wait_done(40, timeout=120)
    cluster.assert_exactly_once(st, 40)
    assert st["node_deaths"] == 0, f"healthy nodes declared dead under 300±50 ms delay: {st}"
    record.update(st)


def test_packet_loss_is_absorbed(cluster, record):
    cluster.c.netem("node-1", loss_pct=15)
    cluster.c.netem("node-2", loss_pct=15)
    cluster.submit(40)
    st = cluster.wait_done(40, timeout=120)
    cluster.assert_exactly_once(st, 40)
    record.update(st)


def test_silent_partition_is_detected_and_work_moves(cluster, record):
    cluster.submit(30)
    time.sleep(0.3)  # every node now holds a job
    t0 = time.monotonic()
    cluster.c.partition("node-2", "coordinator")
    cluster.c.wait_until(lambda: cluster.status()["node_deaths"] >= 1, timeout=20, what="partition detected")
    detect_s = time.monotonic() - t0
    st = cluster.wait_done(30)
    cluster.assert_exactly_once(st, 30)
    assert st["redispatches"] >= 1, st
    cluster.c.heal("node-2", "coordinator")
    cluster.c.wait_until(lambda: cluster.status()["nodes_alive"] == 5, timeout=90, what="node-2 rejoined")
    record.update(st, detect_s=round(detect_s, 2))
    assert 3.5 < detect_s < 6.5, f"detected after {detect_s:.1f}s (timeout is 4 s)"


def test_killed_node_releases_its_job_immediately_and_rejoins(cluster, record):
    cluster.submit(30)
    time.sleep(0.3)
    t0 = time.monotonic()
    cluster.c.kill("node-3")
    cluster.c.wait_until(lambda: cluster.status()["redispatches"] >= 1, timeout=10, what="job re-dispatched")
    requeue_s = time.monotonic() - t0
    st = cluster.wait_done(30)
    cluster.assert_exactly_once(st, 30)
    cluster.c.start("node-3")
    cluster.c.wait_until(lambda: cluster.status()["nodes_alive"] == 5, timeout=30, what="node-3 back")
    record.update(st, requeue_s=round(requeue_s, 2))
    assert requeue_s < 2, f"a closed connection should release the job at once ({requeue_s:.1f}s)"


# --------------------------------------------------------------------------- defect regressions
# Each defect is a realistic bug that can be switched back on in the system under test. The harness
# must detect it (test with the bug on) and pass the fixed code (bug off).

def _late_result_after_redispatch(cluster):
    """Freeze a node holding a job past the failure timeout, let the job finish elsewhere, then thaw it
    so its stale result arrives late."""
    cluster.submit(30)
    time.sleep(0.3)
    cluster.c.freeze("node-1")
    cluster.c.wait_until(lambda: cluster.status()["redispatches"] >= 1, timeout=15, what="re-dispatch")
    time.sleep(2)
    cluster.c.thaw("node-1")
    cluster.wait_done(30)
    time.sleep(1.5)  # let the thawed node's late result arrive
    return cluster.status()


@pytest.mark.parametrize("cluster", [{"env": {"BUG_DOUBLE_COUNT": "1"}}], indirect=True)
def test_defect_double_count_is_caught(cluster, record):
    st = _late_result_after_redispatch(cluster)
    record.update(st)
    assert st["completions"] > st["jobs"], f"harness failed to expose double counting: {st}"


def test_defect_double_count_fixed(cluster, record):
    st = _late_result_after_redispatch(cluster)
    record.update(st)
    cluster.assert_exactly_once(st, 30)
    assert st["duplicate_results"] >= 1, "the late result should have arrived (and been ignored)"


def _jittery_network(cluster, seconds=20):
    for n in ALL_NODES:
        cluster.c.netem(n, delay_ms=500, jitter_ms=400)
    cluster.submit(25)
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        time.sleep(1)
    return cluster.status()


@pytest.mark.parametrize("cluster", [{"env": {"BUG_TIGHT_TIMEOUT": "1"}}], indirect=True)
def test_defect_tight_timeout_is_caught(cluster, record):
    st = _jittery_network(cluster)
    record.update(st)
    assert st["node_deaths"] > 0, f"harness failed to expose false failure detection: {st}"


def test_defect_tight_timeout_fixed(cluster, record):
    st = _jittery_network(cluster)
    record.update(st)
    assert st["node_deaths"] == 0, st


def _frozen_holder(cluster, wait_s=25):
    cluster.submit(20)
    time.sleep(0.3)
    cluster.c.freeze("node-1")  # its TCP connection stays open: nothing closes
    try:
        st = cluster.wait_done(20, timeout=wait_s)
        return True, st
    except HarnessError:
        return False, cluster.status()
    finally:
        cluster.c.thaw("node-1")


@pytest.mark.parametrize("cluster", [{"env": {"BUG_CONN_ONLY_DETECT": "1"}}], indirect=True)
def test_defect_connection_only_detection_is_caught(cluster, record):
    finished, st = _frozen_holder(cluster)
    record.update(st, finished=finished)
    assert not finished, f"harness failed to expose the hung job: {st}"
    assert st["done"] == st["jobs"] - 1, st  # exactly the frozen node's job is stuck


def test_defect_connection_only_detection_fixed(cluster, record):
    finished, st = _frozen_holder(cluster)
    record.update(st, finished=finished)
    assert finished, st
