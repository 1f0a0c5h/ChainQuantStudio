"""State machine coordinating strategy, backtest, signal, and maintenance handoffs."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from pathlib import Path
from types import MappingProxyType

from quant_signal_agent.backtesting.performance import (
    REQUIRED_TRADING_CHARTS,
    validate_trading_performance_evidence,
)
from quant_signal_agent.studio.manager import ManagerAgent
from quant_signal_agent.studio.models import (
    AgentActivity,
    ArtifactKind,
    BacktestEvidence,
    MessageLevel,
    StrategyImplementation,
    StudioAgentRole,
    StudioMessage,
    StudioProject,
    StudioSnapshot,
    WorkflowStage,
)

SAFE_PROJECT_ID = re.compile(r"^[a-z0-9][a-z0-9._-]{1,63}$")


class QuantStudio:
    """Coordinate explicit agent handoffs without creating an execution path."""

    def __init__(self, *, clock: Callable[[], datetime] = lambda: datetime.now(UTC)) -> None:
        self._clock = clock
        self._projects: dict[str, StudioProject] = {}
        self._messages: list[StudioMessage] = []
        self._next_sequence = 1
        self.manager = ManagerAgent(self)
        self._emit(
            StudioAgentRole.SYSTEM,
            MessageLevel.INFO,
            "量化工作室控制層已就緒；交易 Agent 維持硬性待機。",
        )

    def submit_strategy(
        self,
        *,
        project_id: str,
        title: str,
        idea: str | None = None,
        spec_path: str | None = None,
        backtest_required: bool = True,
    ) -> StudioProject:
        return self._submit_artifact(
            project_id=project_id,
            title=title,
            idea=idea,
            spec_path=spec_path,
            artifact_kind=ArtifactKind.STRATEGY,
            backtest_required=backtest_required,
        )

    def submit_signal(
        self,
        *,
        project_id: str,
        title: str,
        idea: str | None = None,
        spec_path: str | None = None,
        backtest_required: bool = True,
    ) -> StudioProject:
        return self._submit_artifact(
            project_id=project_id,
            title=title,
            idea=idea,
            spec_path=spec_path,
            artifact_kind=ArtifactKind.SIGNAL,
            backtest_required=backtest_required,
        )

    def _submit_artifact(
        self,
        *,
        project_id: str,
        title: str,
        idea: str | None,
        spec_path: str | None,
        artifact_kind: ArtifactKind,
        backtest_required: bool,
    ) -> StudioProject:
        if not SAFE_PROJECT_ID.fullmatch(project_id):
            raise ValueError("project_id must be a safe stable identifier")
        if project_id in self._projects:
            raise ValueError(f"studio project already exists: {project_id}")
        if not title.strip():
            raise ValueError("project title is required")
        if not (idea and idea.strip()) and not (spec_path and spec_path.strip()):
            raise ValueError("an artifact idea or spec path is required")
        stage = (
            WorkflowStage.SIGNAL_IMPLEMENTATION
            if artifact_kind is ArtifactKind.SIGNAL
            else WorkflowStage.STRATEGY_IMPLEMENTATION
        )
        project = StudioProject(
            project_id=project_id,
            title=title.strip(),
            idea=idea.strip() if idea else None,
            source_spec_path=spec_path.strip() if spec_path else None,
            stage=stage,
            artifact_kind=artifact_kind,
            backtest_required=backtest_required,
        )
        self._projects[project_id] = project
        self._emit(
            StudioAgentRole.STRATEGY,
            MessageLevel.INFO,
            (
                "已接收 Signal 需求，開始建立中性條件 spec、模組與測試。"
                if artifact_kind is ArtifactKind.SIGNAL
                else "已接收交易策略需求，開始建立方向、進出場、風控 spec 與測試。"
            ),
            project_id,
        )
        return project

    def strategy_implemented(self, project_id: str, implementation: StrategyImplementation) -> None:
        project = self._project(project_id)
        if project.stage not in {
            WorkflowStage.SIGNAL_IMPLEMENTATION,
            WorkflowStage.STRATEGY_IMPLEMENTATION,
        }:
            raise RuntimeError(f"expected implementation stage, got {project.stage.value}")
        if not implementation.test_paths:
            raise ValueError("strategy implementation requires deterministic tests")
        project.implementation = implementation
        project.stage = WorkflowStage.REVIEWING_IMPLEMENTATION
        self._emit(
            StudioAgentRole.STRATEGY,
            MessageLevel.SUCCESS,
            (
                f"策略 {implementation.strategy_id} {implementation.version} "
                "已完成並交給 Review Agent。"
            ),
            project_id,
        )
        self._emit(
            StudioAgentRole.REVIEW,
            MessageLevel.INFO,
            "開始獨立檢查規格、實作與確定性測試。",
            project_id,
        )

    def review_implementation(self, project_id: str, *, approved: bool, evidence: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.REVIEWING_IMPLEMENTATION)
        if not evidence.strip():
            raise ValueError("implementation review evidence is required")
        project.implementation_review = evidence.strip()
        project.stage = (
            WorkflowStage.AWAITING_IMPLEMENTATION_APPROVAL
            if approved
            else self._implementation_stage(project)
        )
        self._emit(
            StudioAgentRole.REVIEW,
            MessageLevel.SUCCESS if approved else MessageLevel.WARNING,
            (
                "實作審查通過；等待使用者核准實作。"
                if approved
                else "實作審查未通過；已退回 Strategy Agent。"
            ),
            project_id,
        )

    def approve_implementation(self, project_id: str, *, approved: bool, note: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.AWAITING_IMPLEMENTATION_APPROVAL)
        if not note.strip():
            raise ValueError("implementation approval note is required")
        project.implementation_approved = approved
        if approved and project.backtest_required:
            project.stage = WorkflowStage.BACKTESTING
            target = "Backtest Agent"
        elif approved:
            project.stage = WorkflowStage.AWAITING_LIVE_APPROVAL
            target = "live approval gate"
        else:
            project.stage = self._implementation_stage(project)
            project.implementation = None
            project.implementation_review = None
            target = "Strategy Agent"
        self._emit(
            StudioAgentRole.SYSTEM,
            MessageLevel.SUCCESS if approved else MessageLevel.WARNING,
            f"使用者{'核准' if approved else '退回'}實作；下一站：{target}。備註：{note.strip()}",
            project_id,
        )

    def backtest_completed(self, project_id: str, evidence: BacktestEvidence) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.BACKTESTING)
        if not evidence.chart_paths:
            raise ValueError("backtest evidence requires at least one chart")
        if project.artifact_kind is ArtifactKind.STRATEGY:
            chart_names = {
                key: path
                for path in evidence.chart_paths
                for key in REQUIRED_TRADING_CHARTS
                if Path(path).stem.endswith(key)
            }
            valid, reason = validate_trading_performance_evidence(
                dict(evidence.summary), chart_names
            )
            if not valid:
                raise ValueError(f"trading-strategy backtest evidence invalid: {reason}")
        project.backtest = evidence
        project.stage = WorkflowStage.REVIEWING_BACKTEST
        self._emit(
            StudioAgentRole.BACKTEST,
            MessageLevel.SUCCESS,
            f"回測 {evidence.run_id} 已完成；已交給 Review Agent 獨立驗證。",
            project_id,
        )

    def review_backtest_evidence(self, project_id: str, *, approved: bool, evidence: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.REVIEWING_BACKTEST)
        if not evidence.strip():
            raise ValueError("backtest review evidence is required")
        project.backtest_review = evidence.strip()
        project.stage = (
            WorkflowStage.OPTIMIZING
            if approved and project.artifact_kind is ArtifactKind.STRATEGY
            else WorkflowStage.AWAITING_BACKTEST_APPROVAL
            if approved
            else WorkflowStage.BACKTESTING
        )
        self._emit(
            StudioAgentRole.REVIEW,
            MessageLevel.SUCCESS if approved else MessageLevel.WARNING,
            (
                "回測證據審查通過；交給 Optimization Agent 進行穩健性診斷。"
                if approved and project.artifact_kind is ArtifactKind.STRATEGY
                else "回測證據審查通過；等待使用者核准回測。"
                if approved
                else "回測證據審查未通過；已退回 Backtest Agent。"
            ),
            project_id,
        )

    def optimization_completed(self, project_id: str, *, proposal: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.OPTIMIZING)
        if not proposal.strip():
            raise ValueError("optimization proposal is required")
        project.optimization_proposal = proposal.strip()
        project.stage = WorkflowStage.AWAITING_OPTIMIZATION_APPROVAL
        self._emit(
            StudioAgentRole.OPTIMIZE,
            MessageLevel.SUCCESS,
            "優化診斷已完成；Manager Agent 正等待使用者決定是否修改策略。",
            project_id,
        )

    def approve_optimization(
        self, project_id: str, *, approved: bool, note: str
    ) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.AWAITING_OPTIMIZATION_APPROVAL)
        if not note.strip():
            raise ValueError("optimization approval note is required")
        project.optimization_approved = approved
        if approved:
            project.stage = WorkflowStage.STRATEGY_IMPLEMENTATION
            project.implementation_review = None
            project.implementation_approved = False
            project.backtest = None
            project.backtest_review = None
            project.backtest_approved = False
            project.live_approved = False
            text = "使用者核准優化方案；已交回 Strategy Agent 建立新版本。"
        else:
            project.stage = WorkflowStage.AWAITING_BACKTEST_APPROVAL
            text = "使用者略過優化方案；保留目前版本並等待回測批准。"
        self._emit(
            StudioAgentRole.SYSTEM,
            MessageLevel.SUCCESS if approved else MessageLevel.INFO,
            f"{text} 備註：{note.strip()}",
            project_id,
        )

    def review_backtest(self, project_id: str, *, approved: bool, note: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.AWAITING_BACKTEST_APPROVAL)
        if not note.strip():
            raise ValueError("backtest review note is required")
        project.backtest_approved = approved
        project.user_approved = approved
        if approved:
            project.stage = WorkflowStage.AWAITING_LIVE_APPROVAL
            level = MessageLevel.SUCCESS
            text = "使用者已核准回測；仍須獨立的上線批准。"
        else:
            project.stage = WorkflowStage.BACKTESTING
            project.backtest = None
            project.backtest_review = None
            level = MessageLevel.WARNING
            text = "使用者未核准回測；已退回 Backtest Agent。"
        self._emit(StudioAgentRole.SYSTEM, level, f"{text} 備註：{note.strip()}", project_id)

    def approve_live(self, project_id: str, *, approved: bool, note: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.AWAITING_LIVE_APPROVAL)
        if not note.strip():
            raise ValueError("live approval note is required")
        project.live_approved = approved
        if approved:
            project.stage = (
                WorkflowStage.READY_FOR_SIGNAL
                if project.artifact_kind is ArtifactKind.SIGNAL
                else WorkflowStage.READY_FOR_TRADING
            )
        else:
            project.stage = WorkflowStage.AWAITING_IMPLEMENTATION_APPROVAL
        self._emit(
            StudioAgentRole.SYSTEM,
            MessageLevel.SUCCESS if approved else MessageLevel.WARNING,
            (
                "使用者已核准上線；Signal 仍須另外下達啟動命令。"
                if approved and project.artifact_kind is ArtifactKind.SIGNAL
                else "使用者已核准研究結果；Trading Agent 仍硬鎖定。"
                if approved
                else "使用者拒絕上線；退回實作批准閘門。"
            )
            + f" 備註：{note.strip()}",
            project_id,
        )

    def start_signal_runtime(self, project_id: str) -> None:
        project = self._project(project_id)
        if project.artifact_kind is not ArtifactKind.SIGNAL:
            raise RuntimeError("signal runtime accepts Signal Definitions, not Trading Strategies")
        if (
            not project.implementation_approved
            or not project.live_approved
            or project.implementation is None
            or (
                project.backtest_required
                and (not project.backtest_approved or project.backtest is None)
            )
        ):
            raise RuntimeError("signal runtime requires all applicable approval gates")
        if project.stage not in {WorkflowStage.READY_FOR_SIGNAL, WorkflowStage.SIGNAL_STOPPED}:
            raise RuntimeError(f"cannot start signal runtime from {project.stage.value}")
        project.stage = WorkflowStage.SIGNAL_RUNNING
        project.runtime_error = None
        self._emit(
            StudioAgentRole.SIGNAL,
            MessageLevel.SUCCESS,
            "已啟動核准 Signal 的訊號 Runtime；只發布 advisory SIGNAL。",
            project_id,
        )

    def stop_signal_runtime(self, project_id: str, *, reason: str) -> None:
        project = self._project(project_id)
        if project.stage not in {WorkflowStage.SIGNAL_RUNNING, WorkflowStage.MAINTENANCE}:
            raise RuntimeError("signal runtime is not running")
        project.stage = WorkflowStage.SIGNAL_STOPPED
        self._emit(
            StudioAgentRole.SIGNAL,
            MessageLevel.INFO,
            f"訊號 Runtime 已停止：{reason.strip() or '未提供原因'}。",
            project_id,
        )

    def publish_signal(self, project_id: str, *, fingerprint: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.SIGNAL_RUNNING)
        if not fingerprint.strip():
            raise ValueError("signal fingerprint is required")
        project.signal_count += 1
        self._emit(
            StudioAgentRole.SIGNAL,
            MessageLevel.SUCCESS,
            f"已發布 SIGNAL {fingerprint.strip()}；等待人工判斷。",
            project_id,
        )
        self._emit(
            StudioAgentRole.TRADING,
            MessageLevel.WARNING,
            "收到 SIGNAL 通知，但交易 Agent 為硬性待機，未建立或送出任何訂單。",
            project_id,
        )

    def report_runtime_error(self, project_id: str, *, error: str) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.SIGNAL_RUNNING)
        if not error.strip():
            raise ValueError("runtime error is required")
        project.runtime_error = error.strip()
        project.stage = WorkflowStage.MAINTENANCE
        self._emit(
            StudioAgentRole.MAINTENANCE,
            MessageLevel.ERROR,
            f"接手訊號 Runtime 問題：{project.runtime_error}",
            project_id,
        )

    def resolve_runtime_error(self, project_id: str, *, evidence: str, resume: bool) -> None:
        project = self._project(project_id)
        self._require_stage(project, WorkflowStage.MAINTENANCE)
        if not evidence.strip():
            raise ValueError("maintenance evidence is required")
        project.runtime_error = None
        project.stage = WorkflowStage.SIGNAL_RUNNING if resume else WorkflowStage.SIGNAL_STOPPED
        destination = "訊號 Runtime 已恢復" if resume else "訊號 Runtime 維持停止"
        self._emit(
            StudioAgentRole.MAINTENANCE,
            MessageLevel.SUCCESS,
            f"問題已修復並驗證；{destination}。證據：{evidence.strip()}",
            project_id,
        )

    def snapshot(self) -> StudioSnapshot:
        return StudioSnapshot(
            generated_at=self._now(),
            agents=MappingProxyType(self._agent_activities()),
            projects=tuple(self._projects.values()),
            messages=tuple(self._messages),
        )

    def _agent_activities(self) -> dict[StudioAgentRole, AgentActivity]:
        stages = {project.stage for project in self._projects.values()}
        return {
            StudioAgentRole.MANAGER: AgentActivity.RUNNING,
            StudioAgentRole.STRATEGY: (
                AgentActivity.WORKING
                if stages
                & {
                    WorkflowStage.SIGNAL_IMPLEMENTATION,
                    WorkflowStage.STRATEGY_IMPLEMENTATION,
                }
                else AgentActivity.IDLE
            ),
            StudioAgentRole.REVIEW: (
                AgentActivity.WORKING
                if stages
                & {
                    WorkflowStage.REVIEWING_IMPLEMENTATION,
                    WorkflowStage.REVIEWING_BACKTEST,
                }
                else AgentActivity.IDLE
            ),
            StudioAgentRole.OPTIMIZE: (
                AgentActivity.WORKING
                if WorkflowStage.OPTIMIZING in stages
                else AgentActivity.WAITING_USER
                if WorkflowStage.AWAITING_OPTIMIZATION_APPROVAL in stages
                else AgentActivity.IDLE
            ),
            StudioAgentRole.BACKTEST: (
                AgentActivity.WORKING
                if WorkflowStage.BACKTESTING in stages
                else AgentActivity.WAITING_USER
                if WorkflowStage.AWAITING_BACKTEST_APPROVAL in stages
                else AgentActivity.IDLE
            ),
            StudioAgentRole.SIGNAL: (
                AgentActivity.RUNNING
                if WorkflowStage.SIGNAL_RUNNING in stages
                else AgentActivity.STOPPED
            ),
            StudioAgentRole.MAINTENANCE: (
                AgentActivity.WORKING if WorkflowStage.MAINTENANCE in stages else AgentActivity.IDLE
            ),
            StudioAgentRole.TRADING: AgentActivity.STANDBY,
        }

    @staticmethod
    def _implementation_stage(project: StudioProject) -> WorkflowStage:
        return (
            WorkflowStage.SIGNAL_IMPLEMENTATION
            if project.artifact_kind is ArtifactKind.SIGNAL
            else WorkflowStage.STRATEGY_IMPLEMENTATION
        )

    def _manager_received(self, instruction: str, project_id: str | None) -> None:
        if not instruction.strip():
            raise ValueError("manager instruction is required")
        self._emit(
            StudioAgentRole.MANAGER,
            MessageLevel.INFO,
            f"已接收使用者指示：{instruction.strip()}",
            project_id,
        )

    def _manager_delegated(self, target: StudioAgentRole, project_id: str) -> None:
        self._emit(
            StudioAgentRole.MANAGER,
            MessageLevel.SUCCESS,
            f"已派工給 {target.value} Agent，後續由 Manager 彙整回報。",
            project_id,
        )

    def _manager_reported(self, project_id: str | None) -> None:
        self._emit(
            StudioAgentRole.MANAGER,
            MessageLevel.SUCCESS,
            "已向相關 Agent 蒐集狀態並完成進度報告。",
            project_id,
        )

    def _manager_refused(self, request: str) -> None:
        self._emit(
            StudioAgentRole.MANAGER,
            MessageLevel.WARNING,
            f"Manager 拒絕超出白名單的請求：{request.strip() or '未分類請求'}",
        )

    def _project(self, project_id: str) -> StudioProject:
        try:
            return self._projects[project_id]
        except KeyError as exc:
            raise KeyError(f"unknown studio project: {project_id}") from exc

    @staticmethod
    def _require_stage(project: StudioProject, expected: WorkflowStage) -> None:
        if project.stage is not expected:
            raise RuntimeError(
                f"project {project.project_id} must be {expected.value}, got {project.stage.value}"
            )

    def _emit(
        self,
        agent: StudioAgentRole,
        level: MessageLevel,
        text: str,
        project_id: str | None = None,
    ) -> None:
        self._messages.append(
            StudioMessage(
                sequence=self._next_sequence,
                emitted_at=self._now(),
                agent=agent,
                level=level,
                text=text,
                project_id=project_id,
            )
        )
        self._next_sequence += 1

    def _now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None:
            raise ValueError("studio clock must return a timezone-aware datetime")
        return value.astimezone(UTC)

    @property
    def projects(self) -> Mapping[str, StudioProject]:
        return MappingProxyType(self._projects)
