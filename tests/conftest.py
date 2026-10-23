import json
import subprocess
import time
from pathlib import Path

import pytest

from fleetlab import Cluster, Topology

ROOT = Path(__file__).resolve().parents[1]
TOPOLOGY = ROOT / "topologies" / "inspect.yaml"
RESULTS = ROOT / "results" / "scenarios.json"
_records: dict[str, dict] = {}



@pytest.fixture(scope="session")
def sut_image():
    """Build the system-under-test image once per test session."""
    subprocess.run(["docker", "build", "-q", "-t", "fleetlab-sut:dev", str(ROOT / "sut")], check=True,
                   capture_output=True)
    return "fleetlab-sut:dev"


class Harness:
    """A running cluster plus helpers for the inspect-cluster system under test."""

    def __init__(self, cluster: Cluster, nodes: int):
        self.c = cluster
        self.nodes = nodes

    def status(self) -> dict:
        return self.c.get("coordinator", 8080, "/status")

    def submit(self, n: int):
        self.c.http("coordinator", 8080, f"/jobs?count={n}", method="POST")

    def wait_done(self, n: int, timeout: float = 60) -> dict:
        self.c.wait_until(lambda: self.status()["done"] >= n, timeout=timeout, what=f"{n} jobs done")
        return self.status()

    def assert_exactly_once(self, st: dict, n: int):
        assert st["done"] == n, st
        assert st["completions"] == n, f"jobs counted more than once: {st}"


@pytest.fixture
def cluster(sut_image, request):
    """Start an isolated cluster for one test and always tear it down. Parametrize with
    @pytest.mark.parametrize("cluster", [{"nodes": 5, "env": {...}}], indirect=True)."""
    opts = getattr(request, "param", {}) or {}
    nodes = opts.get("nodes", 5)
    topo = Topology.load(TOPOLOGY, replicas={"node": nodes}, env=opts.get("env"))
    c = Cluster(topo)
    c.up()
    try:
        h = Harness(c, nodes)
        c.wait_until(lambda: h.status()["nodes_alive"] == nodes, timeout=60, what=f"{nodes} nodes registered")
        yield h
    finally:
        c.down()


@pytest.fixture
def record(request):
    """Store measurements from a test; written to results/scenarios.json at the end of the session."""
    data: dict = {}
    yield data
    rep = getattr(request.node, "rep_call", None)
    data["passed"] = bool(rep and rep.passed)
    _records[request.node.name] = data


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    if rep.when == "call":
        item.rep_call = rep


def pytest_sessionfinish(session, exitstatus):
    if _records:
        RESULTS.parent.mkdir(exist_ok=True)
        existing = json.loads(RESULTS.read_text()) if RESULTS.exists() else {}
        existing.update(_records)
        existing["_generated_at"] = time.strftime("%Y-%m-%dT%H:%M:%S")
        RESULTS.write_text(json.dumps(existing, indent=2, sort_keys=True) + "\n")
