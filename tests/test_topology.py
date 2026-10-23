"""Unit tests that need no Docker daemon."""
import pytest

from fleetlab import Topology
from fleetlab.cluster import _to_mib
from fleetlab.topology import LABEL

BASE = {
    "name": "t", "image": "img:1",
    "services": {
        "coordinator": {"command": ["c"], "publish": [8080]},
        "node": {"replicas": 3, "command": ["n"], "env": {"COORDINATOR": "coordinator"}},
    },
    "env": {"LEASE_S": 6},
}


def test_each_replica_becomes_a_named_service_with_stable_hostname():
    c = Topology.from_dict(BASE).compose("abc123")
    assert c["name"] == "fleetlab-abc123"
    assert set(c["services"]) == {"coordinator", "node-1", "node-2", "node-3"}
    n2 = c["services"]["node-2"]
    assert n2["hostname"] == "node-2"
    assert n2["environment"]["NODE_ID"] == "node-2"
    assert n2["environment"]["LEASE_S"] == "6"            # common env applied, stringified
    assert n2["environment"]["COORDINATOR"] == "coordinator"


def test_singleton_gets_a_network_alias_and_ephemeral_loopback_port():
    c = Topology.from_dict(BASE).compose("r")
    co = c["services"]["coordinator"]
    assert co["networks"]["sut"]["aliases"] == ["coordinator"]
    assert co["ports"] == ["127.0.0.1::8080"]              # never exposed beyond localhost
    assert "ports" not in c["services"]["node-1"]


def test_every_container_is_labelled_and_can_shape_traffic():
    c = Topology.from_dict(BASE).compose("run9")
    for spec in c["services"].values():
        assert spec["labels"][LABEL] == "run9"
        assert "NET_ADMIN" in spec["cap_add"]
    assert c["networks"]["sut"]["labels"][LABEL] == "run9"


def test_overrides_for_replicas_and_env():
    t = Topology.from_dict(BASE, replicas={"node": 20}, env={"BUG_DOUBLE_COUNT": "1"})
    assert len(t.instances()) == 21
    assert t.services["coordinator"].env["BUG_DOUBLE_COUNT"] == "1"


@pytest.mark.parametrize("bad, msg", [
    ({"services": {}}, "at least one service"),
    ({"services": {"x": {"command": ["a"]}}}, "no image"),
    ({"image": "i", "services": {"x": {"replicas": 0}}}, "positive integer"),
])
def test_invalid_topologies_are_rejected(bad, msg):
    with pytest.raises(ValueError, match=msg):
        Topology.from_dict(bad)


@pytest.mark.parametrize("s, mib", [("512KiB", 0.5), ("19.5MiB", 19.5), ("1.5GiB", 1536.0), ("1048576B", 1.0)])
def test_docker_memory_units(s, mib):
    assert _to_mib(s) == pytest.approx(mib)
