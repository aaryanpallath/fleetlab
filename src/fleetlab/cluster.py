"""An on-demand containerized environment for one test run.

    with Cluster(Topology.load("topologies/inspect.yaml")) as c:
        c.wait_until(lambda: c.get("coordinator", 8080, "/status")["nodes_alive"] == 20)
        c.delay("node-3", 300, jitter_ms=50)
        c.partition("node-4", "coordinator")
        ...
    # always torn down here, even if the test failed

Each run is an isolated Compose project with a unique name and a label, so runs can execute in
parallel on one server and anything left behind by a crashed run can be swept.
"""
from __future__ import annotations

import atexit
import json
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.request
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Self

import yaml

from .topology import LABEL, Topology


class HarnessError(RuntimeError):
    pass


def docker(*args: str, check: bool = True, timeout: float = 300, input_: str | None = None) -> str:
    if not shutil.which("docker"):
        raise HarnessError("docker CLI not found")
    p = subprocess.run(["docker", *args], capture_output=True, text=True, timeout=timeout, input=input_, check=False)
    if check and p.returncode != 0:
        raise HarnessError(f"docker {' '.join(args[:4])}... failed: {p.stderr.strip()[-600:]}")
    return p.stdout


@dataclass
class Timing:
    up_s: float = 0.0       # `compose up` returned (containers created and started)
    healthy_s: float = 0.0  # the readiness condition first held
    down_s: float = 0.0


class Cluster:
    def __init__(self, topology: Topology, run_id: str | None = None):
        self.topology = topology
        self.run_id = run_id or uuid.uuid4().hex[:8]
        self.project = f"fleetlab-{self.run_id}"
        self.dir = Path(tempfile.mkdtemp(prefix=f"{self.project}-"))
        self.compose_file = self.dir / "compose.yaml"
        self.timing = Timing()
        self._t0 = 0.0
        self._up = False
        self._ips: dict[str, str] = {}
        self._ports: dict[tuple[str, int], int] = {}

    # ------------------------------------------------------------------ lifecycle
    def compose(self, *args: str, timeout: float = 600) -> str:
        return docker("compose", "-p", self.project, "-f", str(self.compose_file), *args, timeout=timeout)

    def up(self) -> Cluster:
        self.compose_file.write_text(yaml.safe_dump(self.topology.compose(self.run_id), sort_keys=False))
        atexit.register(self.down)
        self._t0 = time.monotonic()
        self._up = True
        self.compose("up", "-d", "--quiet-pull")
        self.timing.up_s = time.monotonic() - self._t0
        return self

    def down(self) -> None:
        if not self._up:
            return
        self._up = False
        t = time.monotonic()
        try:
            self.compose("down", "-v", "--remove-orphans", "-t", "1")
        finally:
            self.timing.down_s = time.monotonic() - t
            shutil.rmtree(self.dir, ignore_errors=True)

    def __enter__(self) -> Self:
        # make Ctrl-C / SIGTERM tear the environment down instead of leaking containers
        try:
            signal.signal(signal.SIGTERM, lambda *_: (self.down(), sys.exit(143)))
        except ValueError:
            pass  # not on the main thread
        return self.up()

    def __exit__(self, *exc) -> None:
        self.down()

    def wait_until(self, predicate: Callable[[], bool], timeout: float = 120, interval: float = 0.25,
                   what: str = "condition") -> float:
        """Poll until predicate() is true; returns seconds since `up` started. Errors count as false."""
        deadline = time.monotonic() + timeout
        last_err: Exception | None = None
        while time.monotonic() < deadline:
            try:
                if predicate():
                    elapsed = time.monotonic() - self._t0
                    if not self.timing.healthy_s:
                        self.timing.healthy_s = elapsed
                    return elapsed
            except Exception as e:  # noqa: BLE001 - services may still be starting
                last_err = e
            time.sleep(interval)
        raise HarnessError(f"timed out after {timeout}s waiting for {what}" + (f" (last error: {last_err})" if last_err else ""))

    # ------------------------------------------------------------------ introspection
    def container(self, instance: str) -> str:
        cid = self.compose("ps", "-q", instance).strip()
        if not cid:
            raise HarnessError(f"no running container for {instance}")
        return cid

    def ip(self, instance: str) -> str:
        if instance not in self._ips:
            out = docker("inspect", "-f", "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
                         self.container(instance))
            self._ips[instance] = out.strip()
        return self._ips[instance]

    def port(self, instance: str, container_port: int) -> int:
        key = (instance, container_port)
        if key not in self._ports:
            out = self.compose("port", instance, str(container_port)).strip()
            self._ports[key] = int(out.rsplit(":", 1)[1])
        return self._ports[key]

    def http(self, instance: str, container_port: int, path: str, method: str = "GET",
             timeout: float = 5) -> Any:
        url = f"http://127.0.0.1:{self.port(instance, container_port)}{path}"
        req = urllib.request.Request(url, method=method, data=b"" if method == "POST" else None)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read())

    def get(self, instance: str, container_port: int, path: str) -> Any:
        return self.http(instance, container_port, path)

    def logs(self, instance: str, tail: int = 200) -> str:
        return self.compose("logs", "--no-color", "--tail", str(tail), instance)

    def stats(self) -> dict[str, dict[str, float]]:
        """Memory (MiB) and CPU (%) per container, one sample."""
        ids = self.compose("ps", "-q").split()
        if not ids:
            return {}
        out = docker("stats", "--no-stream", "--format", "{{json .}}", *ids, timeout=120)
        res = {}
        for line in out.splitlines():
            d = json.loads(line)
            used = d["MemUsage"].split("/")[0].strip()
            res[d["Name"].removeprefix(self.project + "-").rsplit("-1", 1)[0] or d["Name"]] = {
                "mem_mib": _to_mib(used), "cpu_pct": float(d["CPUPerc"].rstrip("%") or 0)}
        return res

    # ------------------------------------------------------------------ faults
    def _exec(self, instance: str, *cmd: str, check: bool = True) -> str:
        return docker("exec", self.container(instance), *cmd, check=check)

    def netem(self, instance: str, *, delay_ms: int = 0, jitter_ms: int = 0, loss_pct: float = 0.0) -> None:
        """Delay / jitter / loss on every packet the instance sends (tc netem on eth0)."""
        args = ["tc", "qdisc", "replace", "dev", "eth0", "root", "netem"]
        if delay_ms:
            args += ["delay", f"{delay_ms}ms"] + ([f"{jitter_ms}ms", "distribution", "normal"] if jitter_ms else [])
        if loss_pct:
            args += ["loss", f"{loss_pct}%"]
        self._exec(instance, *args)

    def clear_netem(self, instance: str) -> None:
        self._exec(instance, "tc", "qdisc", "del", "dev", "eth0", "root", check=False)

    def partition(self, a: str, b: str) -> None:
        """Drop all traffic between two instances in both directions (no RST: a silent network split)."""
        ip_a, ip_b = self.ip(a), self.ip(b)
        for inst, peer in ((a, ip_b), (b, ip_a)):
            self._exec(inst, "iptables", "-A", "INPUT", "-s", peer, "-j", "DROP")
            self._exec(inst, "iptables", "-A", "OUTPUT", "-d", peer, "-j", "DROP")

    def heal(self, *instances: str) -> None:
        for inst in instances or self.topology.instances():
            self._exec(inst, "iptables", "-F", check=False)
            self.clear_netem(inst)

    def freeze(self, instance: str) -> None:
        """Suspend every process in the container (cgroup freezer): the TCP connection stays open."""
        docker("pause", self.container(instance))

    def thaw(self, instance: str) -> None:
        docker("unpause", self.container(instance))

    def kill(self, instance: str) -> None:
        self.compose("kill", instance)

    def start(self, instance: str) -> None:
        self.compose("start", instance)


def _to_mib(s: str) -> float:
    units = {"B": 1 / 2**20, "KiB": 1 / 1024, "MiB": 1, "GiB": 1024, "kB": 1e3 / 2**20, "MB": 1e6 / 2**20,
             "GB": 1e9 / 2**20}
    for u in sorted(units, key=len, reverse=True):
        if s.endswith(u):
            return float(s[: -len(u)]) * units[u]
    return float(s)


def sweep() -> int:
    """Remove containers and networks left by crashed runs (anything carrying the fleetlab label)."""
    ids = docker("ps", "-aq", "--filter", f"label={LABEL}").split()
    if ids:
        docker("rm", "-f", *ids)
    nets = docker("network", "ls", "-q", "--filter", f"label={LABEL}").split()
    if nets:
        docker("network", "rm", *nets, check=False)
    return len(ids)
