from datetime import UTC, datetime

import pytest

from quant_signal_agent.studio import (
    AgentActivity,
    BacktestEvidence,
    ManagerCapability,
    StrategyImplementation,
    StudioAgentRole,
    WorkflowStage,
)
from quant_signal_agent.studio.workflow import QuantStudio

NOW = datetime(2026, 9, 11, 10, 0, tzinfo=UTC)


def implementation() -> StrategyImplementation:
    return StrategyImplementation(
        strategy_id="strategy-2",
        version="1.0.0",
        spec_path="strategies/strategy-2/spec.md",
        module_path="src/quant_signal_agent/strategies/strategy_2.py",
        test_paths=("tests/test_strategy_2.py",),
    )


def evidence() -> BacktestEvidence:
    return BacktestEvidence.create(
        run_id="bt-strategy-2-001",
        manifest_path="reports/backtests/strategy-2/001/manifest.json",
        chart_paths=tuple(
            f"reports/backtests/strategy-2/001/{name}.svg"
            for name in (
                "cagr_roi",
                "max_drawdown",
                "sharpe_ratio",
                "win_rate_payoff_ratio",
                "trade_count",
            )
        ),
        summary={
            "initial_equity_usd": 10_000,
            "cagr": 0.12,
            "roi": 0.26,
            "max_drawdown": -0.09,
            "sharpe_ratio": 1.3,
            "win_rate": 0.53,
            "payoff_ratio": 1.4,
            "trade_count": 120,
        },
    )


def approved_studio() -> QuantStudio:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_signal(project_id="strategy-2", title="Breakout", idea="volume breakout")
    studio.strategy_implemented("strategy-2", implementation())
    studio.review_implementation("strategy-2", approved=True, evidence="tests verified")
    studio.approve_implementation("strategy-2", approved=True, note="implementation accepted")
    studio.backtest_completed("strategy-2", evidence())
    studio.review_backtest_evidence("strategy-2", approved=True, evidence="no lookahead")
    studio.review_backtest("strategy-2", approved=True, note="結果符合預期")
    studio.approve_live("strategy-2", approved=True, note="live accepted")
    return studio


def test_strategy_intake_requires_idea_or_spec() -> None:
    studio = QuantStudio(clock=lambda: NOW)

    with pytest.raises(ValueError, match="idea or spec"):
        studio.submit_strategy(project_id="strategy-2", title="Breakout")

    project = studio.submit_strategy(
        project_id="strategy-3",
        title="Mean reversion",
        spec_path="strategies/strategy-3/spec.md",
    )

    assert project.stage is WorkflowStage.STRATEGY_IMPLEMENTATION
    assert studio.snapshot().agents[StudioAgentRole.STRATEGY] is AgentActivity.WORKING


def test_manager_is_single_user_entry_and_delegates_strategy_work() -> None:
    studio = QuantStudio(clock=lambda: NOW)

    project = studio.manager.submit_strategy(
        project_id="strategy-3",
        title="Mean reversion",
        instruction="請把均值回歸想法實作並回測",
        idea="volume-aware mean reversion",
    )

    snapshot = studio.snapshot()
    assert project.stage is WorkflowStage.STRATEGY_IMPLEMENTATION
    assert snapshot.agents[StudioAgentRole.MANAGER] is AgentActivity.RUNNING
    assert snapshot.messages[1].agent is StudioAgentRole.MANAGER
    assert snapshot.messages[2].agent is StudioAgentRole.STRATEGY
    assert snapshot.messages[3].agent is StudioAgentRole.MANAGER


def test_manager_collects_project_report_without_bypassing_gates() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.manager.submit_signal(
        project_id="strategy-2",
        title="Breakout",
        instruction="建立成交量突破策略",
        idea="volume breakout",
    )
    studio.strategy_implemented("strategy-2", implementation())
    studio.review_implementation("strategy-2", approved=True, evidence="tests verified")
    studio.manager.approve_implementation(
        "strategy-2", approved=True, note="implementation accepted"
    )
    studio.backtest_completed("strategy-2", evidence())
    studio.review_backtest_evidence("strategy-2", approved=True, evidence="no lookahead")

    report = studio.manager.request_report(project_id="strategy-2")

    assert report.stage is WorkflowStage.AWAITING_BACKTEST_APPROVAL
    assert report.responsible_agent is StudioAgentRole.MANAGER
    assert report.as_dict()["responsible_agent"] == "manager"
    assert "使用者核准：否" in report.facts

    studio.manager.review_backtest("strategy-2", approved=True, note="結果符合預期")
    studio.manager.approve_live("strategy-2", approved=True, note="准許啟動")
    studio.manager.start_signal_runtime("strategy-2")
    studio.manager.stop_signal_runtime("strategy-2", reason="使用者要求")

    assert studio.projects["strategy-2"].stage is WorkflowStage.SIGNAL_STOPPED


def test_manager_workspace_report_keeps_trading_locked() -> None:
    studio = QuantStudio(clock=lambda: NOW)

    report = studio.manager.request_report()

    assert report.responsible_agent is StudioAgentRole.MANAGER
    assert report.stage is None
    assert "交易 Agent 維持安全鎖定" in report.facts[-1]


@pytest.mark.parametrize("request_kind", ["modify_core_code", "unrelated_question", "trade"])
def test_manager_rejects_requests_outside_strict_allowlist(request_kind: str) -> None:
    studio = QuantStudio(clock=lambda: NOW)

    with pytest.raises(PermissionError, match="only accepts"):
        studio.manager.authorize_request(request_kind)

    message = studio.snapshot().messages[-1]
    assert message.agent is StudioAgentRole.MANAGER
    assert message.level.value == "warning"
    assert "拒絕超出白名單" in message.text
    assert studio.projects == {}


def test_manager_accepts_only_the_three_declared_capabilities() -> None:
    studio = QuantStudio(clock=lambda: NOW)

    assert {
        studio.manager.authorize_request(capability.value) for capability in ManagerCapability
    } == set(ManagerCapability)


def test_strategy_handoff_requires_tests_and_backtest_requires_charts() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_signal(project_id="strategy-2", title="Breakout", idea="volume breakout")
    without_tests = StrategyImplementation(
        strategy_id="strategy-2",
        version="1.0.0",
        spec_path=None,
        module_path="strategy_2.py",
        test_paths=(),
    )

    with pytest.raises(ValueError, match="deterministic tests"):
        studio.strategy_implemented("strategy-2", without_tests)

    studio.strategy_implemented("strategy-2", implementation())
    studio.review_implementation("strategy-2", approved=True, evidence="tests verified")
    studio.approve_implementation("strategy-2", approved=True, note="implementation accepted")
    without_charts = BacktestEvidence.create(
        run_id="run-1",
        manifest_path="manifest.json",
        chart_paths=(),
        summary={},
    )
    with pytest.raises(ValueError, match="at least one chart"):
        studio.backtest_completed("strategy-2", without_charts)


def test_user_approval_is_required_before_signal_runtime() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_signal(project_id="strategy-2", title="Breakout", idea="volume breakout")
    studio.strategy_implemented("strategy-2", implementation())
    studio.review_implementation("strategy-2", approved=True, evidence="tests verified")
    studio.approve_implementation("strategy-2", approved=True, note="implementation accepted")
    studio.backtest_completed("strategy-2", evidence())
    studio.review_backtest_evidence("strategy-2", approved=True, evidence="no lookahead")

    with pytest.raises(RuntimeError, match="approval gates"):
        studio.start_signal_runtime("strategy-2")

    studio.review_backtest("strategy-2", approved=True, note="核准")
    studio.approve_live("strategy-2", approved=True, note="准許上線")
    studio.start_signal_runtime("strategy-2")

    assert studio.projects["strategy-2"].stage is WorkflowStage.SIGNAL_RUNNING
    assert studio.snapshot().agents[StudioAgentRole.SIGNAL] is AgentActivity.RUNNING


def test_notification_signal_can_skip_backtest_but_not_review_or_live_gate() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_signal(
        project_id="signal-notice",
        title="Status notice",
        idea="notify when state changes",
        backtest_required=False,
    )
    studio.strategy_implemented("signal-notice", implementation())
    assert studio.snapshot().agents[StudioAgentRole.REVIEW] is AgentActivity.WORKING
    studio.review_implementation("signal-notice", approved=True, evidence="tests verified")
    studio.approve_implementation("signal-notice", approved=True, note="accepted")

    assert studio.projects["signal-notice"].stage is WorkflowStage.AWAITING_LIVE_APPROVAL
    assert studio.projects["signal-notice"].backtest is None
    with pytest.raises(RuntimeError, match="approval gates"):
        studio.start_signal_runtime("signal-notice")

    studio.approve_live("signal-notice", approved=True, note="release accepted")
    studio.start_signal_runtime("signal-notice")
    assert studio.projects["signal-notice"].stage is WorkflowStage.SIGNAL_RUNNING


def test_rejected_backtest_returns_to_strategy_agent() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_strategy(project_id="strategy-2", title="Breakout", idea="volume breakout")
    studio.strategy_implemented("strategy-2", implementation())
    studio.review_implementation("strategy-2", approved=True, evidence="tests verified")
    studio.approve_implementation("strategy-2", approved=True, note="implementation accepted")
    studio.backtest_completed("strategy-2", evidence())
    studio.review_backtest_evidence("strategy-2", approved=True, evidence="no lookahead")
    studio.optimization_completed("strategy-2", proposal="Keep current version")
    studio.approve_optimization("strategy-2", approved=False, note="no revision")

    studio.review_backtest("strategy-2", approved=False, note="最大回撤過高")

    project = studio.projects["strategy-2"]
    assert project.stage is WorkflowStage.BACKTESTING
    assert project.implementation is not None
    assert project.backtest is None


def test_approved_optimization_returns_only_strategy_to_new_version_work() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_strategy(project_id="strategy-opt", title="Breakout", idea="volume breakout")
    studio.strategy_implemented("strategy-opt", implementation())
    studio.review_implementation("strategy-opt", approved=True, evidence="tests verified")
    studio.approve_implementation("strategy-opt", approved=True, note="accepted")
    studio.backtest_completed("strategy-opt", evidence())
    studio.review_backtest_evidence("strategy-opt", approved=True, evidence="no lookahead")

    assert studio.projects["strategy-opt"].stage is WorkflowStage.OPTIMIZING
    assert studio.snapshot().agents[StudioAgentRole.OPTIMIZE] is AgentActivity.WORKING

    studio.optimization_completed(
        "strategy-opt", proposal="Add one regime filter with walk-forward validation"
    )
    assert (
        studio.projects["strategy-opt"].stage
        is WorkflowStage.AWAITING_OPTIMIZATION_APPROVAL
    )

    studio.manager.approve_optimization(
        "strategy-opt", approved=True, note="bounded scope accepted"
    )
    project = studio.projects["strategy-opt"]
    assert project.stage is WorkflowStage.STRATEGY_IMPLEMENTATION
    assert project.optimization_approved is True
    assert project.backtest is None
    assert project.implementation is not None


def test_approved_trading_strategy_cannot_enter_signal_runtime() -> None:
    studio = QuantStudio(clock=lambda: NOW)
    studio.submit_strategy(project_id="strategy-2", title="Breakout", idea="long breakout")
    studio.strategy_implemented("strategy-2", implementation())
    studio.review_implementation("strategy-2", approved=True, evidence="tests verified")
    studio.approve_implementation("strategy-2", approved=True, note="implementation accepted")
    studio.backtest_completed("strategy-2", evidence())
    studio.review_backtest_evidence("strategy-2", approved=True, evidence="no lookahead")
    studio.optimization_completed("strategy-2", proposal="Keep current version")
    studio.approve_optimization("strategy-2", approved=False, note="no revision")
    studio.review_backtest("strategy-2", approved=True, note="research approved")
    studio.approve_live("strategy-2", approved=True, note="research release accepted")

    assert studio.projects["strategy-2"].stage is WorkflowStage.READY_FOR_TRADING
    with pytest.raises(RuntimeError, match="not Trading Strategies"):
        studio.start_signal_runtime("strategy-2")


def test_signal_never_routes_to_trading_execution() -> None:
    studio = approved_studio()
    studio.start_signal_runtime("strategy-2")

    studio.publish_signal("strategy-2", fingerprint="strategy-2:BTC:001")

    snapshot = studio.snapshot()
    assert snapshot.trading_enabled is False
    assert snapshot.agents[StudioAgentRole.TRADING] is AgentActivity.STANDBY
    assert snapshot.projects[0].signal_count == 1
    assert "未建立或送出任何訂單" in snapshot.messages[-1].text


def test_runtime_error_hands_control_to_maintenance_and_can_resume() -> None:
    studio = approved_studio()
    studio.start_signal_runtime("strategy-2")

    studio.report_runtime_error("strategy-2", error="event-loop lag")
    assert studio.projects["strategy-2"].stage is WorkflowStage.MAINTENANCE
    assert studio.snapshot().agents[StudioAgentRole.MAINTENANCE] is AgentActivity.WORKING

    studio.resolve_runtime_error("strategy-2", evidence="replay and tests passed", resume=True)

    project = studio.projects["strategy-2"]
    assert project.stage is WorkflowStage.SIGNAL_RUNNING
    assert project.runtime_error is None


def test_snapshot_is_dashboard_serializable() -> None:
    studio = approved_studio()

    payload = studio.snapshot().as_dict()

    assert payload["trading_enabled"] is False
    assert payload["agents"]["trading"] == "standby"
    assert payload["projects"][0]["backtest_run_id"] == "bt-strategy-2-001"
    assert payload["messages"][0]["sequence"] == 1


def test_naive_clock_is_rejected() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        QuantStudio(clock=lambda: datetime(2026, 9, 11, 10, 0))
