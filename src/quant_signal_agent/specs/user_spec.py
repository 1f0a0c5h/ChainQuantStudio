"""Parse the concise user spec and generate the internal normalized contract."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from quant_signal_agent.studio.orchestration import WorkflowDag

FIELD = re.compile(r"^-\s+([^:]+):\s*(.*?)\s*$")
HEADING = re.compile(r"^##\s+\d+\.\s+(.+?)\s*$")
SAFE_ID = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
SEMVER = re.compile(r"^\d+\.\d+\.\d+$")
PLACEHOLDER = re.compile(r"<[^>]+>")


class UserSpecError(ValueError):
    """Raised when the user-facing specification is incomplete or unsafe."""


def _clean(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] == "`":
        return value[1:-1].strip()
    return value


@dataclass(frozen=True, slots=True)
class UserSpec:
    source_path: str
    source_sha256: str
    artifact_type: str
    name: str
    stable_id: str
    version: str
    horizon: str
    backtest_required: bool
    venue: str
    market: str
    symbols_or_universe: str
    timeframes: tuple[str, ...]
    evaluation_event: str
    purpose: str
    exact_behavior: str
    duplicate_reset_rule: str
    missing_stale_gap_behavior: str
    required_data: str
    context_only_data: str
    acceptance_examples: str
    open_questions: str

    def normalized(self) -> dict[str, Any]:
        approvals = ["implementation_approval"]
        if self.backtest_required:
            approvals.append("backtest_approval")
        approvals.append("live_approval")
        return {
            "schema": "chain-normalized-spec/v1",
            "source_sha256": self.source_sha256,
            "artifact": {
                "kind": self.artifact_type,
                "name": self.name,
                "stable_id": self.stable_id,
                "version": self.version,
                "horizon": self.horizon,
                "backtest_required": self.backtest_required,
            },
            "market": {
                "venue": self.venue,
                "market": self.market,
                "symbols_or_universe": self.symbols_or_universe,
                "timeframes": list(self.timeframes),
                "evaluation_event": self.evaluation_event,
                "utc_boundary": True,
            },
            "behavior": {
                "purpose": self.purpose,
                "exact_rules": self.exact_behavior,
                "duplicate_reset_rule": self.duplicate_reset_rule,
                "missing_stale_gap_behavior": self.missing_stale_gap_behavior,
                "required_data": self.required_data,
                "context_only_data": self.context_only_data,
                "acceptance_examples": self.acceptance_examples,
                "open_questions": self.open_questions,
            },
            "workflow": {
                **WorkflowDag.research(backtest_required=self.backtest_required).as_dict(),
                "approval_gates": approvals,
            },
            "safety": {
                "public_market_data_only": True,
                "authenticated_exchange_api": False,
                "order_execution": False,
                "automatic_parameter_promotion": False,
                "fail_closed": True,
            },
        }


def _sections(text: str) -> dict[str, str]:
    values: dict[str, list[str]] = {}
    current: str | None = None
    for raw_line in text.splitlines():
        match = HEADING.match(raw_line.strip())
        if match:
            current = match.group(1).strip().lower()
            values[current] = []
        elif current is not None:
            values[current].append(raw_line.rstrip())
    return {key: "\n".join(lines).strip() for key, lines in values.items()}


def _field_map(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in text.splitlines():
        match = FIELD.match(raw_line.strip())
        if match:
            values[match.group(1).strip().lower()] = _clean(match.group(2))
    return values


def _require(fields: dict[str, str], key: str) -> str:
    value = fields.get(key, "").strip()
    if not value or PLACEHOLDER.search(value):
        raise UserSpecError(f"missing or placeholder field: {key}")
    return value


def _section(sections: dict[str, str], key: str) -> str:
    value = sections.get(key, "").strip()
    content = "\n".join(
        line for line in value.splitlines() if not line.lstrip().startswith(">")
    ).strip()
    if not content or PLACEHOLDER.search(content):
        raise UserSpecError(f"missing or placeholder section: {key}")
    return content


def load_user_spec(path: Path) -> UserSpec:
    text = path.read_text(encoding="utf-8")
    fields = _field_map(text)
    sections = _sections(text)
    if _require(fields, "spec schema") != "chain-user-spec/v1":
        raise UserSpecError("Spec schema must be chain-user-spec/v1")
    artifact_type = _require(fields, "artifact type").lower()
    if artifact_type not in {"signal", "strategy"}:
        raise UserSpecError("Artifact type must be signal or strategy")
    stable_id = _require(fields, "stable id")
    if not SAFE_ID.fullmatch(stable_id):
        raise UserSpecError("Stable ID must be lowercase kebab-case")
    version = _require(fields, "version")
    if not SEMVER.fullmatch(version):
        raise UserSpecError("Version must be semantic major.minor.patch")
    horizon = _require(fields, "horizon").lower()
    if horizon not in {"short", "medium", "long"}:
        raise UserSpecError("Horizon must be short, medium, or long")
    backtest = _require(fields, "backtest required").lower()
    if backtest not in {"yes", "no"}:
        raise UserSpecError("Backtest required must be yes or no")
    if _require(fields, "utc boundary").lower() not in {"yes", "true"}:
        raise UserSpecError("UTC boundary must be yes")
    raw_timeframes = _require(fields, "timeframes")
    timeframes = tuple(
        dict.fromkeys(
            item.strip().lower()
            for item in re.split(r"[,/]", raw_timeframes)
            if item.strip()
        )
    )
    if not timeframes:
        raise UserSpecError("Timeframes must not be empty")
    return UserSpec(
        source_path=str(path.resolve()),
        source_sha256=hashlib.sha256(text.encode()).hexdigest(),
        artifact_type=artifact_type,
        name=_require(fields, "name"),
        stable_id=stable_id,
        version=version,
        horizon=horizon,
        backtest_required=backtest == "yes",
        venue=_require(fields, "venue"),
        market=_require(fields, "market"),
        symbols_or_universe=_require(fields, "symbols or universe"),
        timeframes=timeframes,
        evaluation_event=_require(fields, "evaluation event"),
        purpose=_section(sections, "purpose"),
        exact_behavior=_section(sections, "exact behavior"),
        duplicate_reset_rule=_require(fields, "duplicate/reset rule"),
        missing_stale_gap_behavior=_require(fields, "missing/stale/gap behavior"),
        required_data=_require(fields, "required data"),
        context_only_data=_require(fields, "context-only data"),
        acceptance_examples=_section(sections, "acceptance examples"),
        open_questions=_section(sections, "open questions"),
    )
