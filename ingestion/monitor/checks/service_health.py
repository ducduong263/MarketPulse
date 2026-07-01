"""
Service health check — inspects Docker container states.

Connects to Docker daemon via TCP endpoint (required for Docker Desktop on Windows
where /var/run/docker.sock is not available inside containers).

Enable in Docker Desktop: Settings → General → Expose daemon on tcp://localhost:2375

Each container is checked for status == "running". Missing or exited containers
are reported as DOWN.
"""

import logging
import os
from dataclasses import dataclass

import docker
from docker.errors import NotFound, DockerException

logger = logging.getLogger(__name__)

# ── Docker connection ─────────────────────────────────────────────────────────
# DOCKER_HOST is injected via docker-compose environment.
# Fallback to socket for local dev runs outside Docker.
_DOCKER_HOST = os.getenv("DOCKER_HOST", "unix:///var/run/docker.sock")


# ── Container list ────────────────────────────────────────────────────────────
# Add/remove entries here to change what gets monitored.
# Comment out instead of deleting to preserve intent.
MONITORED_CONTAINERS: list[str] = [
    # ── Core infrastructure ──────────────────────────────────────────────────
    "timescaledb",
    "kafka",
    "zookeeper",
    "schema-registry",
    # ── Critical ingestion (trade + quote only) ──────────────────────────────
    "p-trade",
    "p-quote",
    "c-trade",
    "c-quote",
    "a-trade",
    "a-quote",
    # ── Optional — uncomment to monitor ──────────────────────────────────────
    # "p-index", "c-index",
    # "p-ep",    "c-ep",
    # "p-fi",    "c-fi",
]


@dataclass
class ContainerResult:
    name: str
    status: str          # e.g. "running", "exited", "missing"
    exit_code: int | None
    is_healthy: bool


def check() -> list[ContainerResult]:
    """
    Returns ContainerResult for each monitored container.
    Raises DockerException on daemon connection failure (caller handles escalation).
    """
    results: list[ContainerResult] = []

    client = docker.DockerClient(base_url=_DOCKER_HOST, timeout=10)
    try:
        for name in MONITORED_CONTAINERS:
            try:
                container = client.containers.get(name)
                attrs = container.attrs
                status = container.status  # "running", "exited", "paused", ...
                exit_code = (
                    attrs.get("State", {}).get("ExitCode")
                    if status != "running"
                    else None
                )
                results.append(ContainerResult(
                    name=name,
                    status=status,
                    exit_code=exit_code,
                    is_healthy=(status == "running"),
                ))
            except NotFound:
                results.append(ContainerResult(
                    name=name,
                    status="missing",
                    exit_code=None,
                    is_healthy=False,
                ))
    finally:
        client.close()

    return results


def format_result(r: ContainerResult) -> str:
    """Format single result as a Telegram bullet line."""
    if r.is_healthy:
        return f"📦 `{r.name}` — running"
    if r.status == "missing":
        return f"📦 `{r.name}` — not found"
    suffix = f" (code: {r.exit_code})" if r.exit_code is not None else ""
    return f"📦 `{r.name}` — {r.status}{suffix}"
