"""Single user-facing desk for Quant Studio coordination and reporting."""

from __future__ import annotations

from typing import TYPE_CHECKING

from quant_signal_agent.studio.models import (
    ManagerCapability,
    ManagerReport,
    StudioAgentRole,
    StudioProject,
    WorkflowStage,
)

if TYPE_CHECKING:
    from quant_signal_agent.studio.workflow import QuantStudio


_STAGE_OWNER = {
    WorkflowStage.SIGNAL_IMPLEMENTATION: StudioAgentRole.STRATEGY,
    WorkflowStage.STRATEGY_IMPLEMENTATION: StudioAgentRole.STRATEGY,
    WorkflowStage.REVIEWING_IMPLEMENTATION: StudioAgentRole.REVIEW,
    WorkflowStage.AWAITING_IMPLEMENTATION_APPROVAL: StudioAgentRole.MANAGER,
    WorkflowStage.BACKTESTING: StudioAgentRole.BACKTEST,
    WorkflowStage.REVIEWING_BACKTEST: StudioAgentRole.REVIEW,
    WorkflowStage.OPTIMIZING: StudioAgentRole.OPTIMIZE,
    WorkflowStage.AWAITING_OPTIMIZATION_APPROVAL: StudioAgentRole.MANAGER,
    WorkflowStage.AWAITING_BACKTEST_APPROVAL: StudioAgentRole.MANAGER,
    WorkflowStage.AWAITING_LIVE_APPROVAL: StudioAgentRole.MANAGER,
    WorkflowStage.READY_FOR_SIGNAL: StudioAgentRole.MANAGER,
    WorkflowStage.READY_FOR_TRADING: StudioAgentRole.MANAGER,
    WorkflowStage.SIGNAL_RUNNING: StudioAgentRole.SIGNAL,
    WorkflowStage.MAINTENANCE: StudioAgentRole.MAINTENANCE,
    WorkflowStage.SIGNAL_STOPPED: StudioAgentRole.SIGNAL,
    WorkflowStage.REJECTED: StudioAgentRole.STRATEGY,
}


class ManagerAgent:
    """Facade through which users instruct or inspect the studio.

    Staff completion callbacks remain on :class:`QuantStudio`, while every
    user-originated command enters through this facade and is recorded before
    delegation. The facade never bypasses workflow approval gates.
    """

    def __init__(self, studio: QuantStudio) -> None:
        self._studio = studio

    def authorize_request(self, capability: str) -> ManagerCapability:
        """Validate an external request against the Manager's strict allowlist."""
        try:
            return ManagerCapability(capability)
        except ValueError as exc:
            self._studio._manager_refused(capability)
            raise PermissionError(
                "Manager Agent only accepts add_signal, add_strategy, request_agent_data, "
                "or manage_workflow requests"
            ) from exc

    def submit_signal(
        self,
        *,
        project_id: str,
        title: str,
        instruction: str,
        idea: str | None = None,
        spec_path: str | None = None,
        backtest_required: bool = True,
    ) -> StudioProject:
        self.authorize_request(ManagerCapability.ADD_SIGNAL)
        self._studio._manager_received(instruction, project_id)
        project = self._studio.submit_signal(
            project_id=project_id,
            title=title,
            idea=idea,
            spec_path=spec_path,
            backtest_required=backtest_required,
        )
        self._studio._manager_delegated(StudioAgentRole.STRATEGY, project_id)
        return project

    def submit_strategy(
        self,
        *,
        project_id: str,
        title: str,
        instruction: str,
        idea: str | None = None,
        spec_path: str | None = None,
        backtest_required: bool = True,
    ) -> StudioProject:
        self.authorize_request(ManagerCapability.ADD_STRATEGY)
        self._studio._manager_received(instruction, project_id)
        project = self._studio.submit_strategy(
            project_id=project_id,
            title=title,
            idea=idea,
            spec_path=spec_path,
            backtest_required=backtest_required,
        )
        self._studio._manager_delegated(StudioAgentRole.STRATEGY, project_id)
        return project

    def review_backtest(
        self,
        project_id: str,
        *,
        approved: bool,
        note: str,
    ) -> None:
        self.authorize_request(ManagerCapability.MANAGE_WORKFLOW)
        action = "核准回測" if approved else "退回策略修改"
        self._studio._manager_received(f"{action}：{note}", project_id)
        self._studio.review_backtest(project_id, approved=approved, note=note)
        target = StudioAgentRole.SIGNAL if approved else StudioAgentRole.STRATEGY
        self._studio._manager_delegated(target, project_id)

    def approve_optimization(
        self, project_id: str, *, approved: bool, note: str
    ) -> None:
        self.authorize_request(ManagerCapability.MANAGE_WORKFLOW)
        action = "核准優化方案" if approved else "保留目前版本"
        self._studio._manager_received(f"{action}：{note}", project_id)
        self._studio.approve_optimization(project_id, approved=approved, note=note)
        target = StudioAgentRole.STRATEGY if approved else StudioAgentRole.MANAGER
        self._studio._manager_delegated(target, project_id)

    def approve_implementation(
        self, project_id: str, *, approved: bool, note: str
    ) -> None:
        self.authorize_request(ManagerCapability.MANAGE_WORKFLOW)
        self._studio._manager_received("審核實作批准：" + note, project_id)
        self._studio.approve_implementation(project_id, approved=approved, note=note)
        target = StudioAgentRole.BACKTEST if approved else StudioAgentRole.STRATEGY
        self._studio._manager_delegated(target, project_id)

    def approve_live(self, project_id: str, *, approved: bool, note: str) -> None:
        self.authorize_request(ManagerCapability.MANAGE_WORKFLOW)
        self._studio._manager_received("審核上線批准：" + note, project_id)
        self._studio.approve_live(project_id, approved=approved, note=note)
        target = StudioAgentRole.SIGNAL if approved else StudioAgentRole.STRATEGY
        self._studio._manager_delegated(target, project_id)

    def start_signal_runtime(self, project_id: str) -> None:
        self.authorize_request(ManagerCapability.MANAGE_WORKFLOW)
        self._studio._manager_received("啟動已核准策略的訊號 Runtime", project_id)
        self._studio.start_signal_runtime(project_id)
        self._studio._manager_delegated(StudioAgentRole.SIGNAL, project_id)

    def stop_signal_runtime(self, project_id: str, *, reason: str) -> None:
        self.authorize_request(ManagerCapability.MANAGE_WORKFLOW)
        self._studio._manager_received(f"停止訊號 Runtime：{reason}", project_id)
        self._studio.stop_signal_runtime(project_id, reason=reason)
        self._studio._manager_delegated(StudioAgentRole.SIGNAL, project_id)

    def request_report(self, *, project_id: str | None = None) -> ManagerReport:
        self.authorize_request(ManagerCapability.REQUEST_AGENT_DATA)
        self._studio._manager_received("彙整目前進度與證據", project_id)
        snapshot = self._studio.snapshot()
        latest_messages = tuple(
            message
            for message in snapshot.messages
            if project_id is None or message.project_id in {None, project_id}
        )[-10:]

        if project_id is None:
            signal_running = sum(
                project.stage is WorkflowStage.SIGNAL_RUNNING
                for project in snapshot.projects
            )
            maintenance_active = sum(
                project.stage is WorkflowStage.MAINTENANCE
                for project in snapshot.projects
            )
            active = sum(
                project.stage
                in {
                    WorkflowStage.SIGNAL_IMPLEMENTATION,
                    WorkflowStage.STRATEGY_IMPLEMENTATION,
                    WorkflowStage.REVIEWING_IMPLEMENTATION,
                    WorkflowStage.REVIEWING_BACKTEST,
                    WorkflowStage.OPTIMIZING,
                    WorkflowStage.BACKTESTING,
                    WorkflowStage.SIGNAL_RUNNING,
                    WorkflowStage.MAINTENANCE,
                }
                for project in snapshot.projects
            )
            report = ManagerReport(
                generated_at=snapshot.generated_at,
                scope_project_id=None,
                headline=f"工作室共有 {len(snapshot.projects)} 個專案，{active} 個正在處理。",
                responsible_agent=StudioAgentRole.MANAGER,
                stage=None,
                facts=(
                    f"訊號執行中：{signal_running}",
                    f"維護處理中：{maintenance_active}",
                    "交易 Agent 維持安全鎖定，沒有執行能力。",
                ),
                messages=latest_messages,
            )
        else:
            project = self._studio.projects.get(project_id)
            if project is None:
                raise KeyError(f"unknown studio project: {project_id}")
            backtest_approval = (
                "是"
                if project.backtest_approved
                else "略過"
                if not project.backtest_required
                else "否"
            )
            optimization_approval = (
                "不適用"
                if project.artifact_kind.value == "signal" or not project.backtest_required
                else "是"
                if project.optimization_approved is True
                else "略過"
                if project.optimization_approved is False
                else "待決定"
            )
            report = ManagerReport(
                generated_at=snapshot.generated_at,
                scope_project_id=project_id,
                headline=f"{project.title} 目前位於階段：{project.stage.value}",
                responsible_agent=_STAGE_OWNER[project.stage],
                stage=project.stage,
                facts=(
                    f"使用者核准：{'是' if project.user_approved else '否'}",
                    "批准閘門："
                    f"實作={'是' if project.implementation_approved else '否'}、"
                    f"優化方案={optimization_approval}、"
                    f"回測={backtest_approval}、"
                    f"上線={'是' if project.live_approved else '否'}",
                    f"已發布訊號：{project.signal_count}",
                    f"Runtime 問題：{project.runtime_error or '無'}",
                ),
                messages=latest_messages,
            )

        self._studio._manager_reported(project_id)
        return report
