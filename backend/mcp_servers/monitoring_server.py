from mcp.server.fastmcp import FastMCP
import psutil
from datetime import datetime, timezone
import subprocess
import shutil
```python
"""
Monitoring MCP Server

Exposes real infrastructure signals:

- Host CPU
- Host memory
- Host disk
- Docker container statistics
- Configurable threshold checks
- Safe simulation modes for testing

The orchestrator's Monitoring Agent calls these tools over MCP.
"""


try:
    from settings_store import get_settings
except ImportError:
    from backend.mcp_servers.settings_store import get_settings


mcp = FastMCP("monitoring-agent")


@mcp.tool()
def get_host_metrics() -> dict:
    """Get real-time host CPU, memory, disk, and load metrics."""

    virtual_memory = psutil.virtual_memory()
    disk_usage = psutil.disk_usage("/")

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "cpu_percent": psutil.cpu_percent(interval=0.5),
        "memory": {
            "percent": virtual_memory.percent,
            "used_gb": round(
                virtual_memory.used / (1024**3),
                2,
            ),
            "total_gb": round(
                virtual_memory.total / (1024**3),
                2,
            ),
        },
        "disk": {
            "percent": disk_usage.percent,
            "used_gb": round(
                disk_usage.used / (1024**3),
                2,
            ),
            "total_gb": round(
                disk_usage.total / (1024**3),
                2,
            ),
        },
        "load_avg": (
            list(psutil.getloadavg())
            if hasattr(psutil, "getloadavg")
            else None
        ),
    }


@mcp.tool()
def get_container_stats() -> dict:
    """
    Get live statistics for running Docker containers.

    Returns an empty container list when Docker is unavailable
    or no containers are running.
    """

    if shutil.which("docker") is None:
        return {
            "available": False,
            "reason": "docker CLI not found on host",
            "containers": [],
        }

    try:
        result = subprocess.run(
            [
                "docker",
                "stats",
                "--no-stream",
                "--format",
                "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}|{{.MemPerc}}",
            ],
            capture_output=True,
            text=True,
            timeout=10,
        )

        if result.returncode != 0:
            return {
                "available": False,
                "reason": (
                    result.stderr.strip()
                    or "Docker stats command failed"
                ),
                "containers": [],
            }

        containers = []

        for line in result.stdout.strip().splitlines():
            if not line:
                continue

            parts = line.split("|")

            if len(parts) != 4:
                continue

            name, cpu, mem_usage, mem_pct = parts

            containers.append(
                {
                    "name": name,
                    "cpu_percent": cpu,
                    "mem_usage": mem_usage,
                    "mem_percent": mem_pct,
                }
            )

        return {
            "available": True,
            "containers": containers,
        }

    except Exception as exc:
        return {
            "available": False,
            "reason": str(exc),
            "containers": [],
        }


@mcp.tool()
def check_thresholds(
    cpu_limit: float = 80.0,
    memory_limit: float = 80.0,
    disk_limit: float = 80.0,
    simulation: str = "normal",
) -> dict:
    """
    Evaluate current host metrics against thresholds.

    simulation modes:

    normal:
        Use real host metrics.

    medium:
        Simulate one medium disk breach.

    high:
        Simulate CPU, memory, and disk breaches.

    critical:
        Simulate severe CPU, memory, and disk breaches.

    Simulation is completely safe.
    It does not modify the real machine.
    """

    # Load dashboard settings.
    try:
        settings = get_settings()
    except Exception:
        settings = {}

    if not isinstance(settings, dict):
        settings = {}

    # Read configured thresholds.
    try:
        configured_cpu = settings.get(
            "cpu_limit",
            cpu_limit,
        )
        cpu_limit = float(configured_cpu)
    except (TypeError, ValueError):
        cpu_limit = 80.0

    try:
        configured_memory = settings.get(
            "memory_limit",
            memory_limit,
        )
        memory_limit = float(configured_memory)
    except (TypeError, ValueError):
        memory_limit = 80.0

    try:
        configured_disk = settings.get(
            "disk_limit",
            disk_limit,
        )
        disk_limit = float(configured_disk)
    except (TypeError, ValueError):
        disk_limit = 80.0

    # Get REAL host metrics.
    metrics = get_host_metrics()

    # ---------------------------------------------------------
    # SAFE SIMULATION MODE
    # ---------------------------------------------------------

    if simulation == "normal":
        pass

    elif simulation == "medium":
        metrics["cpu_percent"] = 20.0
        metrics["memory"]["percent"] = 40.0
        metrics["disk"]["percent"] = 86.0

    elif simulation == "high":
        metrics["cpu_percent"] = 90.0
        metrics["memory"]["percent"] = 91.0
        metrics["disk"]["percent"] = 86.0

    elif simulation == "critical":
        metrics["cpu_percent"] = 95.0
        metrics["memory"]["percent"] = 95.0
        metrics["disk"]["percent"] = 110.0

    else:
        return {
            "error": (
                "Invalid simulation mode. "
                "Use: normal, medium, high, or critical."
            )
        }

    # ---------------------------------------------------------
    # THRESHOLD EVALUATION
    # ---------------------------------------------------------

    breaches = []

    if metrics["cpu_percent"] > cpu_limit:
        breaches.append(
            {
                "metric": "cpu",
                "value": metrics["cpu_percent"],
                "limit": cpu_limit,
            }
        )

    if metrics["memory"]["percent"] > memory_limit:
        breaches.append(
            {
                "metric": "memory",
                "value": metrics["memory"]["percent"],
                "limit": memory_limit,
            }
        )

    if metrics["disk"]["percent"] > disk_limit:
        breaches.append(
            {
                "metric": "disk",
                "value": metrics["disk"]["percent"],
                "limit": disk_limit,
            }
        )

    return {
        "timestamp": metrics["timestamp"],
        "healthy": len(breaches) == 0,
        "simulation": simulation,
        "breaches": breaches,
        "metrics": {
            "cpu_percent": metrics["cpu_percent"],
            "memory_percent": metrics["memory"]["percent"],
            "disk_percent": metrics["disk"]["percent"],
        },
        "thresholds": {
            "cpu_limit": cpu_limit,
            "memory_limit": memory_limit,
            "disk_limit": disk_limit,
        },
    }


if __name__ == "__main__":
    mcp.run(transport="stdio")
```
