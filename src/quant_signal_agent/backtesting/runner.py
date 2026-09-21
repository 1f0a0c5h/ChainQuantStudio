"""Reproducible orchestration and manifests for allowlisted backtest engines."""

from __future__ import annotations

import json
import re
import subprocess
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from quant_signal_agent.backtesting.engine import (
    BacktestEngineRegistry,
    EngineContext,
)
from quant_signal_agent.backtesting.generic_spec import GenericBacktestSpec
from quant_signal_agent.backtesting.models import BacktestSpec


def current_revision(project_root: Path) -> str:
    result = subprocess.run(
        ["git", "-c", f"safe.directory={project_root}", "rev-parse", "HEAD"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )
    revision = result.stdout.strip()
    if result.returncode == 0 and re.fullmatch(r"[0-9a-f]{40,64}", revision):
        return revision
    git_dir = project_root / ".git"
    if git_dir.is_file():
        marker = git_dir.read_text(encoding="utf-8").strip()
        if marker.startswith("gitdir: "):
            candidate = Path(marker.removeprefix("gitdir: "))
            git_dir = candidate if candidate.is_absolute() else project_root / candidate
    head_path = git_dir / "HEAD"
    if not head_path.is_file():
        return "unknown"
    head = head_path.read_text(encoding="utf-8").strip()
    if re.fullmatch(r"[0-9a-f]{40,64}", head):
        return head
    if head.startswith("ref: "):
        reference = head.removeprefix("ref: ")
        reference_path = git_dir / reference
        if reference_path.is_file():
            return reference_path.read_text(encoding="utf-8").strip()
        packed_refs = git_dir / "packed-refs"
        if packed_refs.is_file():
            for line in packed_refs.read_text(encoding="utf-8").splitlines():
                if line.endswith(f" {reference}"):
                    return line.split(" ", 1)[0]
    return "unknown"


def current_dirty_state(project_root: Path) -> bool | None:
    result = subprocess.run(
        ["git", "-c", f"safe.directory={project_root}", "status", "--porcelain"],
        cwd=project_root,
        check=False,
        capture_output=True,
        text=True,
    )
    return bool(result.stdout.strip()) if result.returncode == 0 else None


def _write_json(path: Path, value: dict[str, Any]) -> None:
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")
    temporary.replace(path)


@dataclass(frozen=True, slots=True)
class RunOutcome:
    run_id: str
    manifest_path: Path
    status: str


class BacktestRunner:
    def __init__(
        self,
        registry: BacktestEngineRegistry,
        *,
        project_root: Path,
        artifact_root: Path | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        revision: Callable[[Path], str] = current_revision,
        dirty_state: Callable[[Path], bool | None] = current_dirty_state,
    ) -> None:
        self._registry = registry
        self._project_root = project_root.resolve()
        self._artifact_root = (
            artifact_root or self._project_root / "reports" / "backtests"
        ).resolve()
        self._clock = clock
        self._revision = revision
        self._dirty_state = dirty_state

    def plan(self, spec: BacktestSpec | GenericBacktestSpec) -> dict[str, Any]:
        engine = self._registry.resolve(spec.engine)
        engine.validate(spec)
        return {
            "status": "valid",
            "spec_sha256": spec.sha256,
            "strategy_id": spec.strategy.strategy_id,
            "horizon": spec.strategy.horizon.value,
            "engine": engine.engine_id,
            "engine_version": engine.version,
            "data_start": spec.data.start.isoformat(),
            "data_end": spec.data.end.isoformat(),
            "timeframes": list(spec.data.timeframes),
            "trigger_inputs": sorted(spec.data.trigger_inputs),
            "supplemental_inputs": sorted(spec.data.supplemental_inputs),
            "network_required": spec.data.provider != "normalized_csv",
            "auto_promote_parameters": False,
        }

    async def run(self, spec: BacktestSpec | GenericBacktestSpec) -> RunOutcome:
        engine = self._registry.resolve(spec.engine)
        engine.validate(spec)
        started_at = self._clock().astimezone(UTC)
        stamp = started_at.strftime("%Y%m%dT%H%M%SZ")
        run_id = f"{stamp}-{spec.sha256[:12]}"
        artifact_dir = self._artifact_root / spec.strategy.strategy_id / run_id
        artifact_dir.mkdir(parents=True, exist_ok=False)
        manifest_path = artifact_dir / "manifest.json"
        normalized_path = artifact_dir / "normalized-spec.json"
        _write_json(normalized_path, spec.normalized())
        manifest: dict[str, Any] = {
            "schema_version": spec.schema_version,
            "run_id": run_id,
            "status": "running",
            "started_at": started_at.isoformat(),
            "completed_at": None,
            "strategy_id": spec.strategy.strategy_id,
            "strategy_version": spec.strategy.version,
            "horizon": spec.strategy.horizon.value,
            "spec_sha256": spec.sha256,
            "engine": engine.engine_id,
            "engine_version": engine.version,
            "code_revision": self._revision(self._project_root),
            "working_tree_dirty": self._dirty_state(self._project_root),
            "normalized_spec": normalized_path.name,
            "artifacts": {},
            "summary": {},
            "failure_type": None,
        }
        _write_json(manifest_path, manifest)
        try:
            result = await engine.run(
                spec,
                EngineContext(self._project_root, artifact_dir),
            )
        except Exception as error:
            manifest["status"] = "failed"
            manifest["completed_at"] = self._clock().astimezone(UTC).isoformat()
            manifest["failure_type"] = type(error).__name__
            _write_json(manifest_path, manifest)
            raise
        manifest["status"] = "complete"
        manifest["completed_at"] = self._clock().astimezone(UTC).isoformat()
        manifest["artifacts"] = dict(result.artifacts)
        manifest["summary"] = dict(result.summary)
        _write_json(manifest_path, manifest)
        return RunOutcome(run_id, manifest_path, "complete")
