"""Topology files: which services make up the system under test, and how many of each.

    name: inspect-cluster
    image: fleetlab-sut:dev
    services:
      coordinator:
        command: [python, cluster.py, coordinator]
        publish: [8080]            # reachable from the test process on an ephemeral host port
      node:
        replicas: 20
        command: [python, cluster.py, node]
        env: {COORDINATOR: coordinator}
    env: {}                        # applied to every service (e.g. BUG_* switches)

Every replica becomes its own Compose service (node-1 ... node-N) with a stable hostname, so faults
can target one node and logs name it.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

LABEL = "fleetlab.run"


@dataclass
class Service:
    name: str
    command: list[str]
    image: str
    replicas: int = 1
    env: dict[str, str] = field(default_factory=dict)
    publish: list[int] = field(default_factory=list)

    def instance_names(self) -> list[str]:
        return [self.name] if self.replicas == 1 else [f"{self.name}-{i}" for i in range(1, self.replicas + 1)]


@dataclass
class Topology:
    name: str
    services: dict[str, Service]

    @classmethod
    def load(cls, path: str | Path, *, replicas: dict[str, int] | None = None,
             env: dict[str, str] | None = None) -> Topology:
        return cls.from_dict(yaml.safe_load(Path(path).read_text()), replicas=replicas, env=env)

    @classmethod
    def from_dict(cls, d: dict[str, Any], *, replicas: dict[str, int] | None = None,
                  env: dict[str, str] | None = None) -> Topology:
        if not d.get("services"):
            raise ValueError("topology needs at least one service")
        common = {**{k: str(v) for k, v in (d.get("env") or {}).items()}, **(env or {})}
        services = {}
        for name, s in d["services"].items():
            image = s.get("image") or d.get("image")
            if not image:
                raise ValueError(f"service {name!r} has no image")
            n = (replicas or {}).get(name, s.get("replicas", 1))
            if not isinstance(n, int) or n < 1:
                raise ValueError(f"service {name!r}: replicas must be a positive integer")
            services[name] = Service(name=name, command=list(s.get("command") or []), image=image, replicas=n,
                                     env={**common, **{k: str(v) for k, v in (s.get("env") or {}).items()}},
                                     publish=list(s.get("publish") or []))
        return cls(name=d.get("name", "fleetlab"), services=services)

    def compose(self, run_id: str) -> dict[str, Any]:
        """Docker Compose model for one run: one Compose service per replica, all on a private network."""
        out: dict[str, Any] = {"name": f"fleetlab-{run_id}", "services": {},
                               "networks": {"sut": {"driver": "bridge", "labels": {LABEL: run_id}}}}
        for svc in self.services.values():
            for inst in svc.instance_names():
                env = {**svc.env, "NODE_ID": inst}
                spec: dict[str, Any] = {
                    "image": svc.image, "command": svc.command, "hostname": inst, "environment": env,
                    "networks": {"sut": {"aliases": [svc.name] if svc.replicas == 1 else []}},
                    "cap_add": ["NET_ADMIN"],  # tc netem and iptables inside the container
                    "labels": {LABEL: run_id, "fleetlab.service": svc.name},
                    "init": True, "stop_grace_period": "1s",
                }
                if svc.publish:
                    spec["ports"] = [f"127.0.0.1::{p}" for p in svc.publish]
                out["services"][inst] = spec
        return out

    def instances(self) -> list[str]:
        return [i for s in self.services.values() for i in s.instance_names()]
