"""Deterministic control-plane primitives for Cha!n Quant Studio."""

from __future__ import annotations

import hashlib
import json
import re
import threading
from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any


class ApprovalGate(StrEnum):
    IMPLEMENTATION = "implementation_approval"
    OPTIMIZATION = "optimization_approval"
    BACKTEST = "backtest_approval"
    LIVE = "live_approval"


class DagNodeKind(StrEnum):
    AGENT = "agent"
    SERVICE = "service"
    APPROVAL = "approval"


@dataclass(frozen=True, slots=True)
class DagNode:
    node_id: str
    owner: str
    kind: DagNodeKind
    requires: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class WorkflowDag:
    """A serializable DAG; optional nodes are removed, never fake-completed."""

    nodes: tuple[DagNode, ...]

    @classmethod
    def research(
        cls, *, backtest_required: bool, optimization_required: bool = False
    ) -> WorkflowDag:
        if optimization_required and not backtest_required:
            raise ValueError("optimization requires a backtest")
        nodes = [
            DagNode("strategy", "strategy", DagNodeKind.AGENT),
            DagNode(
                "review_implementation",
                "review",
                DagNodeKind.AGENT,
                ("strategy",),
            ),
            DagNode(
                ApprovalGate.IMPLEMENTATION,
                "manager",
                DagNodeKind.APPROVAL,
                ("review_implementation",),
            ),
        ]
        live_dependency = ApprovalGate.IMPLEMENTATION.value
        if backtest_required:
            nodes.extend(
                [
                    DagNode(
                        "dataset",
                        "data_service",
                        DagNodeKind.SERVICE,
                        (ApprovalGate.IMPLEMENTATION,),
                    ),
                    DagNode("backtest", "backtest", DagNodeKind.AGENT, ("dataset",)),
                    DagNode(
                        "review_backtest",
                        "review",
                        DagNodeKind.AGENT,
                        ("backtest",),
                    ),
                ]
            )
            backtest_dependency = "review_backtest"
            if optimization_required:
                nodes.extend(
                    [
                        DagNode(
                            "optimize",
                            "optimize",
                            DagNodeKind.AGENT,
                            ("review_backtest",),
                        ),
                        DagNode(
                            ApprovalGate.OPTIMIZATION,
                            "manager",
                            DagNodeKind.APPROVAL,
                            ("optimize",),
                        ),
                    ]
                )
                backtest_dependency = ApprovalGate.OPTIMIZATION.value
            nodes.append(
                DagNode(
                    ApprovalGate.BACKTEST,
                    "manager",
                    DagNodeKind.APPROVAL,
                    (backtest_dependency,),
                )
            )
            live_dependency = ApprovalGate.BACKTEST.value
        nodes.append(
            DagNode(
                ApprovalGate.LIVE,
                "manager",
                DagNodeKind.APPROVAL,
                (live_dependency,),
            )
        )
        return cls(tuple(nodes))

    def as_dict(self) -> dict[str, Any]:
        return {
            "nodes": [
                {
                    **asdict(node),
                    "kind": node.kind.value,
                    "requires": list(node.requires),
                }
                for node in self.nodes
            ]
        }


@dataclass(frozen=True, slots=True)
class DatasetKey:
    venue: str
    market: str
    symbol: str
    timeframe: str
    start: str
    end: str
    schema_version: str = "normalized-candle-v1"

    @property
    def canonical(self) -> str:
        return "|".join(str(value).strip().lower() for value in asdict(self).values())

    @property
    def dataset_id(self) -> str:
        return hashlib.sha256(self.canonical.encode()).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class DatasetVersion:
    dataset_id: str
    version: str
    path: str
    sha256: str
    byte_count: int
    key: DatasetKey

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["key"] = asdict(self.key)
        return value


class VersionedMarketDataService:
    """Content-addressed local data cache shared by every backtest work order."""

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()
        self.data_root = self.root / "datasets"
        self.registry_path = self.root / "registry.json"
        self._lock = threading.RLock()

    def acquire(self, key: DatasetKey, loader: Callable[[], bytes]) -> DatasetVersion:
        with self._lock:
            registry = self._read_registry()
            existing = registry.get(key.canonical)
            if isinstance(existing, dict):
                version = self._from_dict(existing)
                path = self.root / version.path
                if path.is_file() and self._sha256(path.read_bytes()) == version.sha256:
                    return version

            payload = loader()
            if not payload:
                raise ValueError("market dataset loader returned no bytes")
            digest = self._sha256(payload)
            version_id = digest[:16]
            relative = Path("datasets") / key.dataset_id / f"{version_id}.csv"
            destination = self.root / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            if not destination.exists():
                temporary = destination.with_suffix(".tmp")
                temporary.write_bytes(payload)
                temporary.replace(destination)
            version = DatasetVersion(
                dataset_id=key.dataset_id,
                version=version_id,
                path=relative.as_posix(),
                sha256=digest,
                byte_count=len(payload),
                key=key,
            )
            registry[key.canonical] = version.as_dict()
            self._write_registry(registry)
            return version

    def list_versions(self) -> list[dict[str, Any]]:
        with self._lock:
            return [dict(value) for value in self._read_registry().values()]

    def _read_registry(self) -> dict[str, dict[str, Any]]:
        if not self.registry_path.is_file():
            return {}
        try:
            value = json.loads(self.registry_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        return value if isinstance(value, dict) else {}

    def _write_registry(self, value: dict[str, dict[str, Any]]) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        temporary = self.registry_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
        temporary.replace(self.registry_path)

    @staticmethod
    def _from_dict(value: Mapping[str, Any]) -> DatasetVersion:
        raw_key = value["key"]
        if not isinstance(raw_key, Mapping):
            raise ValueError("invalid data-service registry key")
        return DatasetVersion(
            dataset_id=str(value["dataset_id"]),
            version=str(value["version"]),
            path=str(value["path"]),
            sha256=str(value["sha256"]),
            byte_count=int(value["byte_count"]),
            key=DatasetKey(
                venue=str(raw_key["venue"]),
                market=str(raw_key["market"]),
                symbol=str(raw_key["symbol"]),
                timeframe=str(raw_key["timeframe"]),
                start=str(raw_key["start"]),
                end=str(raw_key["end"]),
                schema_version=str(raw_key["schema_version"]),
            ),
        )

    @staticmethod
    def _sha256(payload: bytes) -> str:
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class ArtifactRecord:
    work_order: str
    artifact_kind: str
    spec_sha256: str | None
    code_revision: str
    dataset_sha256: str | None
    tests: tuple[str, ...]
    reports: tuple[str, ...]
    recorded_at: str
    artifact_id: str | None = None
    artifact_version: str | None = None
    manifest_sha256: str | None = None

    def as_dict(self) -> dict[str, Any]:
        value = asdict(self)
        value["tests"] = list(self.tests)
        value["reports"] = list(self.reports)
        return value


class ArtifactRegistry:
    """Crash-safe traceability registry for implementation and evidence artifacts."""

    def __init__(self, path: Path) -> None:
        self.path = path.resolve()
        self._lock = threading.RLock()

    def upsert(self, record: ArtifactRecord) -> None:
        with self._lock:
            values = {item["work_order"]: item for item in self.list_records()}
            values[record.work_order] = record.as_dict()
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            temporary.write_text(
                json.dumps(list(values.values()), indent=2, sort_keys=True),
                encoding="utf-8",
            )
            temporary.replace(self.path)

    def list_records(self) -> list[dict[str, Any]]:
        with self._lock:
            if not self.path.is_file():
                return []
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                return []
            if not isinstance(value, list):
                return []
            return [dict(item) for item in value if isinstance(item, dict)]

    @staticmethod
    def file_sha256(path: Path | None) -> str | None:
        if path is None or not path.is_file():
            return None
        return hashlib.sha256(path.read_bytes()).hexdigest()


class CircuitState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"


@dataclass(slots=True)
class GlobalCodexCircuitBreaker:
    """One shared quota circuit that prevents every work order retrying independently."""

    default_cooldown_seconds: int = 3600
    state: CircuitState = CircuitState.CLOSED
    open_until: datetime | None = None
    reason: str | None = None

    def is_open(self, now: datetime) -> bool:
        self._require_aware(now)
        if self.state is CircuitState.OPEN and self.open_until and now < self.open_until:
            return True
        if self.state is CircuitState.OPEN:
            self.close()
        return False

    def open(self, reason: str, *, now: datetime) -> datetime:
        self._require_aware(now)
        self.state = CircuitState.OPEN
        self.reason = reason
        self.open_until = self._retry_at(reason, now) or (
            now + timedelta(seconds=self.default_cooldown_seconds)
        )
        return self.open_until

    def close(self) -> None:
        self.state = CircuitState.CLOSED
        self.open_until = None
        self.reason = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "state": self.state.value,
            "open_until": self.open_until.isoformat() if self.open_until else None,
            "reason": self.reason,
        }

    @staticmethod
    def _retry_at(detail: str, now: datetime) -> datetime | None:
        iso_match = re.search(r"try again at\s+([0-9T:+-]{16,35})", detail, re.I)
        if iso_match:
            try:
                value = datetime.fromisoformat(iso_match.group(1))
                return value.astimezone(UTC) if value.tzinfo else None
            except ValueError:
                return None
        dated_match = re.search(
            r"try again at\s+([A-Z][a-z]{2}\s+\d{1,2}(?:st|nd|rd|th)?,\s+"
            r"\d{4}\s+\d{1,2}:\d{2}\s*[AP]M)",
            detail,
            re.I,
        )
        if dated_match:
            normalized = re.sub(r"(\d)(?:st|nd|rd|th)", r"\1", dated_match.group(1))
            try:
                parsed = datetime.strptime(normalized, "%b %d, %Y %I:%M %p")
            except ValueError:
                return None
            return parsed.replace(tzinfo=now.tzinfo)
        time_match = re.search(r"try again at\s+(\d{1,2}:\d{2}\s*[AP]M)", detail, re.I)
        if not time_match:
            return None
        try:
            parsed_time = datetime.strptime(time_match.group(1).upper(), "%I:%M %p").time()
        except ValueError:
            return None
        candidate = datetime.combine(now.date(), parsed_time, tzinfo=now.tzinfo)
        return candidate if candidate > now else candidate + timedelta(days=1)

    @staticmethod
    def _require_aware(value: datetime) -> None:
        if value.tzinfo is None:
            raise ValueError("circuit-breaker clock must be timezone-aware")


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def unique_strings(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(dict.fromkeys(str(value) for value in values if str(value).strip()))
