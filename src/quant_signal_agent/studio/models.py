"""Typed state shared by the quant studio agents and dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any


class StudioAgentRole(StrEnum):
    MANAGER = "manager"
    STRATEGY = "strategy"
    BACKTEST = "backtest"
    REVIEW = "review"
    OPTIMIZE = "optimize"
    SIGNAL = "signal"
    MAINTENANCE = "maintenance"
    TRADING = "trading"
    SYSTEM = "system"


class ManagerCapability(StrEnum):
    """The complete set of requests accepted by the Manager Agent."""

    ADD_SIGNAL = "add_signal"
    ADD_STRATEGY = "add_strategy"
    REQUEST_AGENT_DATA = "request_agent_data"
    MANAGE_WORKFLOW = "manage_workflow"


class AgentActivity(StrEnum):
    IDLE = "idle"
    WORKING = "working"
    WAITING_USER = "waiting_user"
    RUNNING = "running"
    STOPPED = "stopped"
    STANDBY = "standby"


class WorkflowStage(StrEnum):
    SIGNAL_IMPLEMENTATION = "signal_implementation"
    STRATEGY_IMPLEMENTATION = "strategy_implementation"
    REVIEWING_IMPLEMENTATION = "reviewing_implementation"
    AWAITING_IMPLEMENTATION_APPROVAL = "awaiting_implementation_approval"
    BACKTESTING = "backtesting"
    REVIEWING_BACKTEST = "reviewing_backtest"
    OPTIMIZING = "optimizing"
    AWAITING_OPTIMIZATION_APPROVAL = "awaiting_optimization_approval"
    AWAITING_BACKTEST_APPROVAL = "awaiting_backtest_approval"
    AWAITING_LIVE_APPROVAL = "awaiting_live_approval"
    READY_FOR_SIGNAL = "ready_for_signal"
    READY_FOR_TRADING = "ready_for_trading"
    SIGNAL_RUNNING = "signal_running"
    MAINTENANCE = "maintenance"
    SIGNAL_STOPPED = "signal_stopped"
    REJECTED = "rejected"


class MessageLevel(StrEnum):
    INFO = "info"
    SUCCESS = "success"
    WARNING = "warning"
    ERROR = "error"


class ArtifactKind(StrEnum):
    SIGNAL = "signal"
    STRATEGY = "strategy"


@dataclass(frozen=True, slots=True)
class StrategyImplementation:
    strategy_id: str
    version: str
    spec_path: str | None
    module_path: str
    test_paths: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BacktestEvidence:
    run_id: str
    manifest_path: str
    chart_paths: tuple[str, ...]
    summary: MappingProxyType[str, float | int | str | None]

    @classmethod
    def create(
        cls,
        *,
        run_id: str,
        manifest_path: str,
        chart_paths: tuple[str, ...],
        summary: dict[str, float | int | str | None],
    ) -> BacktestEvidence:
        return cls(run_id, manifest_path, chart_paths, MappingProxyType(dict(summary)))


@dataclass(frozen=True, slots=True)
class StudioMessage:
    sequence: int
    emitted_at: datetime
    agent: StudioAgentRole
    level: MessageLevel
    text: str
    project_id: str | None = None


@dataclass(slots=True)
class StudioProject:
    project_id: str
    title: str
    idea: str | None
    source_spec_path: str | None
    stage: WorkflowStage
    implementation: StrategyImplementation | None = None
    backtest: BacktestEvidence | None = None
    backtest_required: bool = True
    implementation_review: str | None = None
    backtest_review: str | None = None
    optimization_proposal: str | None = None
    optimization_approved: bool | None = None
    implementation_approved: bool = False
    backtest_approved: bool = False
    live_approved: bool = False
    user_approved: bool = False
    runtime_error: str | None = None
    signal_count: int = 0
    artifact_kind: ArtifactKind = ArtifactKind.STRATEGY


@dataclass(frozen=True, slots=True)
class StudioSnapshot:
    generated_at: datetime
    agents: MappingProxyType[StudioAgentRole, AgentActivity]
    projects: tuple[StudioProject, ...]
    messages: tuple[StudioMessage, ...]
    trading_enabled: bool = field(default=False, init=False)

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "trading_enabled": self.trading_enabled,
            "agents": {role.value: activity.value for role, activity in self.agents.items()},
            "projects": [
                {
                    "project_id": project.project_id,
                    "title": project.title,
                    "artifact_kind": project.artifact_kind.value,
                    "stage": project.stage.value,
                    "strategy_id": (
                        project.implementation.strategy_id if project.implementation else None
                    ),
                    "strategy_version": (
                        project.implementation.version if project.implementation else None
                    ),
                    "backtest_run_id": project.backtest.run_id if project.backtest else None,
                    "backtest_required": project.backtest_required,
                    "approvals": {
                        "implementation": project.implementation_approved,
                        "optimization": project.optimization_approved,
                        "backtest": project.backtest_approved,
                        "live": project.live_approved,
                    },
                    "user_approved": project.user_approved,
                    "runtime_error": project.runtime_error,
                    "signal_count": project.signal_count,
                }
                for project in self.projects
            ],
            "messages": [
                {
                    "sequence": message.sequence,
                    "emitted_at": message.emitted_at.isoformat(),
                    "agent": message.agent.value,
                    "level": message.level.value,
                    "text": message.text,
                    "project_id": message.project_id,
                }
                for message in self.messages
            ],
        }


@dataclass(frozen=True, slots=True)
class ManagerReport:
    """User-facing status assembled by the Manager Agent from studio state."""

    generated_at: datetime
    scope_project_id: str | None
    headline: str
    responsible_agent: StudioAgentRole
    stage: WorkflowStage | None
    facts: tuple[str, ...]
    messages: tuple[StudioMessage, ...]

    def as_dict(self) -> dict[str, Any]:
        return {
            "generated_at": self.generated_at.isoformat(),
            "scope_project_id": self.scope_project_id,
            "headline": self.headline,
            "responsible_agent": self.responsible_agent.value,
            "stage": self.stage.value if self.stage else None,
            "facts": list(self.facts),
            "messages": [
                {
                    "sequence": message.sequence,
                    "emitted_at": message.emitted_at.isoformat(),
                    "agent": message.agent.value,
                    "level": message.level.value,
                    "text": message.text,
                    "project_id": message.project_id,
                }
                for message in self.messages
            ],
        }
