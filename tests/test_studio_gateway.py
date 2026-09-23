from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from aiohttp import FormData
from aiohttp.test_utils import TestClient, TestServer

from quant_signal_agent.studio.gateway import (
    CodexManagerRouter,
    CodexWorkOrderRunner,
    JsonStateStore,
    ManagerDecision,
    SignalRuntimeController,
    StudioService,
    _codex_account_is_ready,
    _ensure_codex_home,
    create_app,
)
from quant_signal_agent.studio.orchestration import ArtifactRecord, ArtifactRegistry


def _review_report(
    stage: str, verdict: str = "PASS", blockers: tuple[str, ...] = ()
) -> str:
    checks = CodexWorkOrderRunner._REVIEW_CHECKS[stage]
    return "\n".join(
        [
            f"# {verdict}",
            f"REVIEW_STAGE: {stage}",
            *(f"CHECK: {category} | verified in test fixture" for category in checks),
            *(f"BLOCKER: {blocker}" for blocker in blockers),
        ]
    )


def _optimization_report(verdict: str = "IMPROVE") -> str:
    return "\n".join(
        [
            f"# {verdict}",
            "OPTIMIZATION_STAGE: post_backtest",
            "DIAGNOSIS: [drawdown-cluster] losses cluster in sideways regimes",
            "PROPOSED_CHANGE: add one explicit regime filter",
            "OVERFIT_GUARD: validate once on untouched chronological test data",
            "RISK: fewer trades and possible missed breakouts",
            "EXPECTED_EVIDENCE: lower test drawdown without collapsing trade count",
        ]
    )


def test_codex_account_readiness_uses_account_presence_not_provider_flag() -> None:
    assert _codex_account_is_ready(
        SimpleNamespace(account={"type": "chatgpt"}, requires_openai_auth=True)
    )
    assert not _codex_account_is_ready(SimpleNamespace(account=None, requires_openai_auth=True))
    assert _codex_account_is_ready(SimpleNamespace(account=None, requires_openai_auth=False))


def test_codex_home_honors_override_and_uses_current_device_user(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    custom = tmp_path / "custom-codex"
    monkeypatch.setenv("CODEX_HOME", str(custom))
    assert _ensure_codex_home(tmp_path / "ignored-user") == custom

    monkeypatch.delenv("CODEX_HOME")
    device_user_home = tmp_path / "device-user"
    assert _ensure_codex_home(device_user_home) == device_user_home / ".codex"
    assert os.environ["CODEX_HOME"] == str(device_user_home / ".codex")


def test_codex_manager_router_rejects_unauthenticated_client_before_thread(
    tmp_path: Path,
) -> None:
    class FakeAccount:
        account = None
        requires_openai_auth = True

    class FakeClient:
        thread_started = False

        async def account(self) -> FakeAccount:
            return FakeAccount()

        async def thread_start(self, **kwargs: Any) -> None:
            del kwargs
            self.thread_started = True
            raise AssertionError("thread must not start without authentication")

    async def scenario() -> None:
        client = FakeClient()
        router = CodexManagerRouter(project_root=tmp_path)
        router._client = client  # type: ignore[assignment]
        try:
            await router.route("新增 BTC 4h EMA 策略", {})
        except RuntimeError as exc:
            assert str(exc) == "Codex account authentication required"
        else:
            raise AssertionError("authentication failure should be surfaced")
        assert client.thread_started is False

    asyncio.run(scenario())

class FakeProcess:
    def __init__(self, pid: int = 4321) -> None:
        self.pid = pid
        self.returncode: int | None = None
        self._done = asyncio.Event()

    async def wait(self) -> int:
        await self._done.wait()
        assert self.returncode is not None
        return self.returncode

    def finish(self, returncode: int = 0) -> None:
        self.returncode = returncode
        self._done.set()


def test_signal_runtime_uses_fixed_module_and_shutdown_file(tmp_path: Path) -> None:
    async def scenario() -> None:
        calls: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
        process = FakeProcess()

        async def create_process(*args: Any, **kwargs: Any) -> FakeProcess:
            calls.append((args, kwargs))
            return process

        controller = SignalRuntimeController(
            project_root=tmp_path,
            runtime_dir=tmp_path / ".runtime",
            process_factory=create_process,
        )
        controller.select_signal("sample-signal", "1.1.0")
        status = await controller.start()
        assert status.running
        assert status.pid == 4321
        assert calls[0][0][1:] == ("-m", "quant_signal_agent.main")
        shutdown_file = Path(calls[0][1]["env"]["QSA_SHUTDOWN_FILE"])
        environment = calls[0][1]["env"]
        assert environment["QSA_ACTIVE_SIGNAL_ID"] == "sample-signal"
        assert environment["QSA_ACTIVE_SIGNAL_VERSION"] == "1.1.0"

        async def finish_after_request() -> None:
            await asyncio.sleep(0.01)
            assert await asyncio.to_thread(shutdown_file.is_file)
            process.finish()

        finisher = asyncio.create_task(finish_after_request())
        stopped = await controller.stop(timeout_seconds=1)
        await finisher
        assert not stopped.running
        metadata = json.loads((tmp_path / ".runtime" / "signal-runtime.json").read_text())
        assert metadata["state"] == "stopped"
        assert metadata["signal_id"] == "sample-signal"
        assert metadata["signal_version"] == "1.1.0"
        await controller.close()

    asyncio.run(scenario())


def test_signal_runtime_does_not_adopt_legacy_or_reused_pid(tmp_path: Path) -> None:
    runtime_dir = tmp_path / ".runtime"
    runtime_dir.mkdir()
    metadata_path = runtime_dir / "signal-runtime.json"
    metadata_path.write_text(
        json.dumps(
            {
                "state": "running",
                "pid": os.getpid(),
                "started_at": "2026-01-01T00:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    controller = SignalRuntimeController(project_root=tmp_path, runtime_dir=runtime_dir)
    status = controller.status()

    assert not status.running
    assert status.pid is None
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    assert metadata["state"] == "stopped"
    assert "stopped_at" in metadata


def test_studio_message_ids_remain_monotonic_after_history_is_trimmed() -> None:
    state: dict[str, Any] = {"messages": []}

    for index in range(205):
        StudioService._append_message(state, "MANAGER", f"message-{index}")

    identifiers = [item["id"] for item in state["messages"]]
    assert len(identifiers) == 200
    assert identifiers == list(range(6, 206))


def test_manager_starts_real_runtime_and_rejects_trading(tmp_path: Path) -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.running = False

        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(
                self.running,
                99 if self.running else None,
                None,
                "x.log",
                "live" if self.running else "stopped",
            )

        async def start(self) -> Any:
            self.running = True
            return self.status()

        async def stop(self, *, timeout_seconds: float = 45.0) -> Any:
            del timeout_seconds
            self.running = False
            return self.status()

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        runtime = FakeRuntime()
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=runtime,  # type: ignore[arg-type]
        )
        refused = await service.handle_manager_message("幫我下單 BTC")
        assert refused["denied"] is True
        assert runtime.running is False

        queued = await service.handle_manager_message("新增一個 20 日均線突破策略")
        assert queued["action"] == "add_strategy"
        assert queued["snapshot"]["work_orders"][0]["status"] == "queued"
        assert queued["snapshot"]["work_orders"][0]["artifact_kind"] == "strategy"
        assert list((tmp_path / "work" / "studio").glob("*/request.json"))

        signal = await service.handle_manager_message("新增一個中性的波動擴張 signal")
        assert signal["action"] == "add_signal"
        assert signal["handoff"] == "strategy"
        assert signal["snapshot"]["work_orders"][1]["artifact_kind"] == "signal"

        maintenance = await service.handle_manager_message("診斷並修復 Signal runtime 錯誤")
        assert maintenance["action"] == "manage_workflow"
        assert maintenance["handoff"] == "maintenance"
        assert maintenance["snapshot"]["work_orders"][2]["agent"] == "maintenance"

    asyncio.run(scenario())


def test_manager_starts_only_exact_live_approved_ema_version(tmp_path: Path) -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.selected = None
            self.running = False

        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(self.running, 42 if self.running else None, None, "x", "x")

        def select_signal(self, signal_id: str, version: str | None) -> None:
            self.selected = (signal_id, version)

        async def start(self) -> Any:
            self.running = True
            return self.status()

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        artifact = tmp_path / "signals" / "implemented" / "sample-signal"
        artifact.mkdir(parents=True)
        manifest_path = artifact / "1.1.0.toml"
        manifest_path.write_text('version = "1.1.0"\n', encoding="utf-8")
        store = JsonStateStore(tmp_path / "state.json")
        registry = ArtifactRegistry(tmp_path / ".runtime" / "studio-artifacts.json")
        registry.upsert(
            ArtifactRecord(
                artifact_id="sample-signal",
                artifact_version="1.1.0",
                artifact_kind="signal",
                work_order="signal-adopt-1",
                spec_sha256="spec-hash",
                code_revision="test-revision",
                dataset_sha256=None,
                tests=("test_studio_gateway",),
                reports=(),
                manifest_sha256=hashlib.sha256(manifest_path.read_bytes()).hexdigest(),
                recorded_at="2026-09-17T00:00:00+00:00",
            )
        )
        runtime = FakeRuntime()
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=runtime,  # type: ignore[arg-type]
            artifact_registry=registry,
        )

        denied = await service.handle_manager_message("run sample-signal signal")
        assert denied["denied"] is True
        assert runtime.selected is None

        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-adopt-1",
                        "artifact_kind": "signal",
                        "artifact_id": "sample-signal",
                        "artifact_version": "1.1.0",
                        "operation": "adopt",
                        "status": "approved",
                        "stage": "ready_for_signal",
                        "approvals": {"live": {"approved": True}},
                    }
                ],
            }
        )
        started = await service.handle_manager_message("run sample-signal signal")
        assert started["denied"] is False
        assert runtime.selected == ("sample-signal", "1.1.0")
        assert runtime.running

    asyncio.run(scenario())


def test_semantic_router_decision_controls_dispatch_without_keyword_rules(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.running = False

        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(self.running, 77 if self.running else None, None, "x", "x")

        async def start(self) -> Any:
            self.running = True
            return self.status()

        async def close(self) -> None:
            return None

    class FakeRouter:
        def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def route(self, instruction: str, context: dict[str, Any]) -> ManagerDecision:
            assert instruction == "把已核准的第一套東西跑起來"
            assert context["approved_signals"] == []
            assert context["trading_strategies"] == []
            return ManagerDecision(
                "start_signal",
                "signal",
                "sample-signal",
                rationale="semantic match",
                confidence=0.96,
            )

    async def scenario() -> None:
        runtime = FakeRuntime()
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=runtime,  # type: ignore[arg-type]
            manager_router=FakeRouter(),
        )
        result = await service.handle_manager_message("把已核准的第一套東西跑起來")
        assert result["action"] is None
        assert result["denied"] is True
        assert result["decision"]["confidence"] == 0.96
        assert not runtime.running

    asyncio.run(scenario())


def test_live_approved_created_signal_is_available_to_manager_runtime_start(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def __init__(self) -> None:
            self.running = False
            self.selected: tuple[str, str | None] | None = None

        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(self.running, 78 if self.running else None, None, "x", "x")

        def select_signal(self, signal_id: str, version: str | None) -> None:
            self.selected = (signal_id, version)

        async def start(self) -> Any:
            self.running = True
            return self.status()

        async def close(self) -> None:
            return None

    class FakeRouter:
        async def route(self, instruction: str, context: dict[str, Any]) -> ManagerDecision:
            assert instruction == "run sample-signal"
            assert context["approved_signals"] == ["sample-signal"]
            return ManagerDecision(
                "start_signal", "signal", "sample-signal", confidence=0.99
            )

    async def scenario() -> None:
        manifest = tmp_path / "signals" / "implemented" / "sample-signal" / "1.1.0.toml"
        manifest.parent.mkdir(parents=True)
        manifest.write_text(
            'stable_id = "sample-signal"\nversion = "1.1.0"\n', encoding="utf-8"
        )
        registry = ArtifactRegistry(tmp_path / ".runtime" / "studio-artifacts.json")
        registry.upsert(
            ArtifactRecord(
                work_order="signal-create-1",
                artifact_kind="signal",
                spec_sha256="a" * 64,
                code_revision="revision",
                dataset_sha256=None,
                tests=("tests/test_signal.py",),
                reports=(),
                recorded_at="2026-09-17T00:00:00+00:00",
                artifact_id="sample-signal",
                artifact_version="1.1.0",
                manifest_sha256=ArtifactRegistry.file_sha256(manifest),
            )
        )
        store = JsonStateStore(tmp_path / ".runtime" / "studio-state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-create-1",
                        "agent": "strategy",
                        "artifact_kind": "signal",
                        "artifact_id": "sample-signal",
                        "artifact_version": "1.1.0",
                        "operation": "create",
                        "status": "approved",
                        "stage": "ready_for_signal",
                        "approvals": {"live": {"approved": True}},
                    }
                ],
            }
        )
        runtime = FakeRuntime()
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=runtime,  # type: ignore[arg-type]
            manager_router=FakeRouter(),  # type: ignore[arg-type]
            artifact_registry=registry,
        )
        result = await service.handle_manager_message("run sample-signal")
        assert result["action"] == "start_signal"
        assert runtime.selected == ("sample-signal", "1.1.0")
        assert runtime.running is True

        manifest.write_text('stable_id = "tampered"\n', encoding="utf-8")
        state = await store.read()
        assert service._approved_signal_versions(state) == {}

    asyncio.run(scenario())


def test_manager_preserves_natural_language_skip_backtest_preference(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )

        result = await service.handle_manager_message("新增一個 BTC 波動擴張 signal，不需要回測")

        order = result["snapshot"]["work_orders"][0]
        assert result["action"] == "add_signal"
        assert result["decision"]["backtest_required"] is False
        assert order["backtest_required"] is False
        assert all(node["node_id"] != "backtest" for node in order["dag"]["nodes"])
        assert "略過回測" in result["reply"]

    asyncio.run(scenario())


def test_manager_can_create_versioned_modification_of_existing_signal(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        existing = tmp_path / "signals" / "implemented" / "sample-signal"
        existing.mkdir(parents=True)
        (existing / "spec.md").write_text("# EMA 200 touch\n", encoding="utf-8")
        (existing / "1.0.0.toml").write_text('stable_id = "sample-signal"\n', encoding="utf-8")
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )

        result = await service.handle_manager_message(
            "修改 sample-signal signal 的通知文字，不用回測"
        )

        order = result["snapshot"]["work_orders"][0]
        assert result["action"] == "modify_signal"
        assert result["handoff"] == "strategy"
        assert order["operation"] == "modify"
        assert order["artifact_id"] == "sample-signal"
        assert order["backtest_required"] is False
        assert existing.is_dir()
        assert "原版本保持不變" in result["reply"]

    asyncio.run(scenario())


def test_existing_artifact_adoption_starts_at_independent_review(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        manifest = tmp_path / "signals" / "implemented" / "sample-signal" / "1.0.0.toml"
        manifest.parent.mkdir(parents=True)
        manifest.write_text('stable_id = "sample-signal"\nversion = "1.0.0"\n')
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )

        result = await service.handle_artifact_adoption(
            artifact_kind="signal",
            artifact_id="sample-signal",
            version="1.0.0",
            backtest_required=False,
            note="reuse completed implementation",
        )

        order = result["work_order"]
        assert order["operation"] == "adopt"
        assert order["status"] == "queued"
        assert order["stage"] == "review_implementation"
        assert order["artifact_version"] == "1.0.0"
        assert order["backtest_required"] is False
        assert "strategy" in order["results"]
        assert all(node["node_id"] != "backtest" for node in order["dag"]["nodes"])

    asyncio.run(scenario())


def test_stop_backtest_agent_cancels_active_backtest_and_targets_handoff(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-active-backtest-1",
                        "agent": "strategy",
                        "artifact_kind": "signal",
                        "status": "working",
                        "stage": "backtest",
                        "created_at": "2026-09-15T10:33:39+00:00",
                        "results": {"strategy": "complete"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=lambda *_: None,  # type: ignore[arg-type]
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            worker=worker,
        )

        result = await service.handle_manager_message("stop backtest agent")

        order = result["snapshot"]["work_orders"][0]
        assert result["action"] == "cancel_work_order"
        assert result["handoff"] == "backtest"
        assert result["decision"]["target_agent"] == "backtest"
        assert order["status"] == "cancelled"
        assert order["stage"] == "cancelled"

    asyncio.run(scenario())


def test_manager_uses_bounded_fallback_when_semantic_router_is_unavailable(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x", "stopped")

        async def close(self) -> None:
            return None

    class FailingRouter:
        def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def route(self, instruction: str, context: dict[str, Any]) -> ManagerDecision:
            del instruction, context
            raise TimeoutError("router timed out")

    async def scenario() -> None:
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            manager_router=FailingRouter(),
        )
        result = await service.handle_manager_message(
            "新增研究用 Trading Strategy，規格在 strategies/proposals/example/spec.md；"
            "不得啟動 Signal Runtime、不得自動交易"
        )
        assert result["action"] == "add_strategy"
        assert result["decision"]["confidence"] == 0.9
        assert "bounded fallback" in result["decision"]["rationale"]
        assert result["snapshot"]["work_orders"][0]["artifact_kind"] == "strategy"

    asyncio.run(scenario())


def test_named_signal_stop_survives_router_failure_without_stopping_another_signal(
    tmp_path: Path,
) -> None:
    from quant_signal_agent.studio.gateway import RuntimeStatus

    class FakeRuntime:
        def __init__(self) -> None:
            self.active_signal = "sample-signal"
            self.stop_calls = 0

        def status(self) -> RuntimeStatus:
            running = self.active_signal is not None
            return RuntimeStatus(
                running,
                42 if running else None,
                None,
                "x.log",
                "running" if running else "stopped",
                signal_id=self.active_signal,
            )

        async def stop(self) -> RuntimeStatus:
            self.stop_calls += 1
            self.active_signal = None
            return self.status()

        async def close(self) -> None:
            return None

    class FailingRouter:
        async def route(self, instruction: str, context: dict[str, Any]) -> ManagerDecision:
            del instruction, context
            raise RuntimeError("Codex account authentication required")

    async def scenario() -> None:
        artifact = tmp_path / "signals" / "implemented" / "sample-signal"
        artifact.mkdir(parents=True)
        (artifact / "1.1.0.toml").write_text('version = "1.1.0"\n', encoding="utf-8")
        runtime = FakeRuntime()
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=runtime,  # type: ignore[arg-type]
            manager_router=FailingRouter(),  # type: ignore[arg-type]
        )

        stopped = await service.handle_manager_message("stop sample-signal")
        assert stopped["action"] == "stop_signal"
        assert stopped["denied"] is False
        assert "bounded fallback" in stopped["decision"]["rationale"]
        assert runtime.stop_calls == 1
        assert stopped["snapshot"]["signal_runtime"]["running"] is False

        runtime.active_signal = "sample-signal"
        stopped_again = await service.handle_manager_message("stop sample-signal")
        assert stopped_again["denied"] is False
        assert runtime.stop_calls == 2
        assert runtime.active_signal is None

    asyncio.run(scenario())


def test_manager_keeps_clarification_when_router_and_fallback_are_ambiguous(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x", "stopped")

        async def close(self) -> None:
            return None

    class FailingRouter:
        def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def route(self, instruction: str, context: dict[str, Any]) -> ManagerDecision:
            del instruction, context
            raise RuntimeError("router unavailable")

    async def scenario() -> None:
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            manager_router=FailingRouter(),
        )
        result = await service.handle_manager_message("幫我處理一下")
        assert result["denied"] is True
        assert result["decision"]["action"] == "clarify"
        assert result["snapshot"]["work_orders"] == []

    asyncio.run(scenario())


def test_codex_worker_hands_strategy_to_backtest_and_waits_for_review(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        calls: list[tuple[str, str | None]] = []

        async def fake_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            assert order["instruction"] == "新增一個 20 日均線突破策略"
            if role == "backtest":
                assert "chain-data-service-handoff/v1" in order["results"]["dataset"]
            calls.append((role, prior_result))
            if role == "review":
                return _review_report(order["stage"])
            if role == "optimize":
                return _optimization_report()
            return f"{role}-complete"

        store = JsonStateStore(tmp_path / "state.json")
        async def fake_dataset(_order: dict[str, Any]) -> str:
            return '{"schema":"chain-data-service-handoff/v1","datasets":[{"sha256":"' + (
                "a" * 64
            ) + '"}]}'

        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=fake_turn,
            dataset_materializer=fake_dataset,
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            worker=worker,
        )
        await service.handle_manager_message("新增一個 20 日均線突破策略")
        worker.start()
        for _ in range(100):
            snapshot = await service.snapshot()
            if snapshot["work_orders"][0]["status"] == "awaiting_implementation_approval":
                break
            await asyncio.sleep(0.01)
        identifier = snapshot["work_orders"][0]["id"]
        await service.handle_work_order_approval(
            identifier,
            gate="implementation_approval",
            approved=True,
            note="implementation accepted",
        )
        for _ in range(100):
            snapshot = await service.snapshot()
            if snapshot["work_orders"][0]["status"] == "awaiting_optimization_approval":
                break
            await asyncio.sleep(0.01)
        await service.handle_work_order_approval(
            identifier,
            gate="optimization_approval",
            approved=False,
            note="keep current tested version",
        )
        snapshot = await service.snapshot()
        await worker.close()

        order = snapshot["work_orders"][0]
        assert calls == [
            ("strategy", None),
            ("review", "strategy-complete"),
            ("backtest", "strategy-complete"),
            ("review", "backtest-complete"),
            ("optimize", "backtest-complete"),
        ]
        assert order["stage"] == "backtest_approval"
        assert "chain-data-service-handoff/v1" in order["results"]["dataset"]
        assert order["results"]["backtest"] == "backtest-complete"
        assert (tmp_path / "work" / "studio" / order["id"] / "result.json").is_file()

    asyncio.run(scenario())


def test_codex_worker_skips_backtest_only_when_user_explicitly_opts_out(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def fake_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del prior_result
            calls.append(role)
            return (
                _review_report(order["stage"])
                if role == "review"
                else f"{role}-complete"
            )

        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-no-backtest-1",
                        "agent": "strategy",
                        "artifact_kind": "signal",
                        "backtest_required": False,
                        "status": "queued",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "instruction": "新增價格狀態通知",
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=fake_turn)
        worker.start()
        for _ in range(100):
            order = (await store.read())["work_orders"][0]
            if order["status"] == "awaiting_implementation_approval":
                break
            await asyncio.sleep(0.01)
        await worker.close()

        assert calls == ["strategy", "review"]
        assert order["stage"] == "implementation_approval"
        assert "backtest" not in order["results"]
        messages = (await store.read())["messages"]
        assert any("等待實作批准" in item["text"] for item in messages)

    asyncio.run(scenario())


def test_backtest_stage_recovers_missing_data_service_handoff(tmp_path: Path) -> None:
    async def scenario() -> None:
        materialized: list[str] = []

        async def fake_dataset(order: dict[str, Any]) -> str:
            materialized.append(order["id"])
            return '{"schema":"chain-data-service-handoff/v1","datasets":[]}'

        async def fake_turn(
            order: dict[str, Any], role: str, prior_result: str | None
        ) -> str:
            del prior_result
            if role == "backtest":
                assert "chain-data-service-handoff/v1" in order["results"]["dataset"]
                return "backtest-complete"
            return _review_report(order["stage"])

        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-missing-dataset-1",
                        "agent": "strategy",
                        "artifact_kind": "strategy",
                        "backtest_required": True,
                        "status": "queued",
                        "stage": "backtest",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "instruction": "backtest target strategy",
                        "results": {"strategy": "implementation handoff"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=fake_turn,
            dataset_materializer=fake_dataset,
        )

        await worker._execute((await store.read())["work_orders"][0])

        order = (await store.read())["work_orders"][0]
        assert materialized == ["strategy-missing-dataset-1"]
        assert order["status"] == "awaiting_backtest_approval"
        assert "chain-data-service-handoff/v1" in order["results"]["dataset"]

    asyncio.run(scenario())


def test_optimization_approval_archives_evidence_and_queues_versioned_modification(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        proposal = _optimization_report()
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-optimize-1",
                        "agent": "strategy",
                        "artifact_kind": "strategy",
                        "artifact_id": "breakout",
                        "artifact_version": "1.0.0",
                        "operation": "create",
                        "backtest_required": True,
                        "optimization_required": True,
                        "status": "awaiting_optimization_approval",
                        "stage": "optimization_approval",
                        "approvals": {
                            "implementation": {"approved": True},
                            "optimization": None,
                            "backtest": None,
                            "live": None,
                        },
                        "results": {
                            "strategy": "v1 handoff",
                            "review_implementation": "# PASS",
                            "backtest": "v1 evidence",
                            "review_backtest": "# PASS",
                            "optimization": proposal,
                        },
                        "checkpoints": [],
                    }
                ],
            }
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )

        await service.handle_work_order_approval(
            "strategy-optimize-1",
            gate="optimization_approval",
            approved=True,
            note="apply the bounded regime filter proposal",
        )

        order = (await store.read())["work_orders"][0]
        assert order["status"] == "queued"
        assert order["stage"] == "strategy"
        assert order["operation"] == "modify"
        assert order["artifact_version"] == "1.0.0"
        assert order["results"] == {"optimization": proposal}
        assert order["approvals"] == {
            "implementation": None,
            "optimization": None,
            "backtest": None,
            "live": None,
        }
        assert order["optimization_history"][0]["artifact_version"] == "1.0.0"
        assert order["optimization_history"][0]["approved"] is True

    asyncio.run(scenario())


def test_optimization_report_requires_diagnosis_safeguards_and_evidence() -> None:
    complete = CodexWorkOrderRunner._optimization_report_complete
    assert complete(_optimization_report())
    assert complete(_optimization_report("KEEP"))
    assert not complete("# IMPROVE\nOPTIMIZATION_STAGE: post_backtest")
    assert not complete(_optimization_report().replace("OVERFIT_GUARD:", "NOTE:"))


def test_unfinished_legacy_strategy_backtest_gains_optimizer_without_touching_terminal_orders(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "work_orders": [
                    {
                        "id": "strategy-open",
                        "agent": "strategy",
                        "artifact_kind": "strategy",
                        "backtest_required": True,
                        "status": "dead_letter",
                        "approvals": {"implementation": {"approved": True}},
                    },
                    {
                        "id": "strategy-terminal",
                        "agent": "strategy",
                        "artifact_kind": "strategy",
                        "backtest_required": True,
                        "status": "approved",
                        "approvals": {},
                    },
                ]
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)
        await worker._migrate_optimization_workflow()
        orders = (await store.read())["work_orders"]
        assert orders[0]["optimization_required"] is True
        assert orders[0]["approvals"]["optimization"] is None
        assert any(
            node["node_id"] == "optimize" for node in orders[0]["dag"]["nodes"]
        )
        assert "optimization_required" not in orders[1]

    asyncio.run(scenario())


def test_codex_worker_routes_failed_review_back_to_strategy_with_evidence(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        calls: list[tuple[str, str | None]] = []

        async def fake_turn(
            order: dict[str, Any], role: str, prior_result: str | None
        ) -> str:
            calls.append((role, prior_result))
            return (
                _review_report(
                    order["stage"],
                    "FAIL",
                    ("re-arm semantics are incorrect", "boundary regression test is missing"),
                )
                if role == "review"
                else "built"
            )

        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-review-fail-1",
                        "agent": "strategy",
                        "artifact_kind": "signal",
                        "backtest_required": False,
                        "status": "working",
                        "stage": "strategy",
                        "created_at": "2026-01-01T00:00:00+00:00",
                        "instruction": "add signal",
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=fake_turn)

        await worker._execute((await store.read())["work_orders"][0])

        order = (await store.read())["work_orders"][0]
        assert calls == [("strategy", None), ("review", "built")]
        assert order["status"] == "queued"
        assert order["stage"] == "strategy"
        assert order["attempts"] == 1
        assert order["results"]["review_implementation"].startswith("# FAIL")
        assert order["results"]["review_implementation"].count("BLOCKER:") == 2
        assert "Review Agent returned FAIL" in order["last_error"]

        prompt = worker._prompt(
            order,
            role="strategy",
            prior_result=order["results"]["review_implementation"],
        )
        assert "Independent Review Agent findings" in prompt
        assert "re-arm semantics are incorrect" in prompt
        assert "boundary regression test is missing" in prompt

    asyncio.run(scenario())


def test_default_work_order_retry_budget_is_thirty_or_six_same_issue(tmp_path: Path) -> None:
    worker = CodexWorkOrderRunner(
        project_root=tmp_path,
        store=JsonStateStore(tmp_path / "state.json"),
        agent_turn=lambda *_args, **_kwargs: None,  # type: ignore[arg-type]
    )
    assert worker.max_attempts == 30
    assert worker.same_issue_limit == 5


def test_review_same_issue_is_dead_lettered_on_sixth_occurrence(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write({"messages": [], "work_orders": [{
            "id": "strategy-repeat-1", "agent": "strategy", "status": "working",
            "stage": "review_backtest", "attempts": 0, "results": {},
        }]})
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)
        for number in range(1, 7):
            report = _review_report(
                "review_backtest", "FAIL",
                (f"[wrong-venue] Spot data cannot validate USD-M (review {number})",),
            )
            await worker._complete_review(
                "strategy-repeat-1", "review_backtest", {"review_backtest": report}
            )
            state = await store.read()
            order = state["work_orders"][0]
            assert order["attempts"] == number
            assert order["issue_counts"]["review_backtest:wrong-venue"] == number
            assert order["status"] == ("dead_letter" if number == 6 else "queued")
        assert state["dead_letters"][0]["recurring_issues"] == [
            "review_backtest:wrong-venue"
        ]
        assert "Spot data cannot validate USD-M" in state["messages"][-1]["text"]

    asyncio.run(scenario())


def test_distinct_failures_reach_total_limit_at_thirty(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write({"messages": [], "work_orders": [{
            "id": "strategy-total-1", "agent": "strategy", "status": "working",
            "stage": "backtest", "attempts": 0, "results": {},
        }]})
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)
        for number in range(1, 31):
            await worker._requeue(
                "strategy-total-1", "backtest", {}, f"distinct failure {number}"
            )
            order = (await store.read())["work_orders"][0]
            assert order["status"] == ("dead_letter" if number == 30 else "queued")
        assert len(order["issue_counts"]) == 30

    asyncio.run(scenario())


def test_review_human_blocker_pauses_and_notifies_manager(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write({"messages": [], "work_orders": [{
            "id": "strategy-user-1", "agent": "strategy", "status": "working",
            "stage": "review_backtest", "attempts": 0, "results": {},
        }]})
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)
        report = _review_report(
            "review_backtest", "FAIL", ("[missing-private-data] dataset unavailable",)
        ) + (
            "\nUSER_ACTION_REQUIRED: Please supply the licensed dataset "
            "or approve a public substitute"
        )
        await worker._complete_review(
            "strategy-user-1", "review_backtest", {"review_backtest": report}
        )
        state = await store.read()
        assert state["work_orders"][0]["status"] == "awaiting_user_input"
        assert state["work_orders"][0]["stage"] == "backtest"
        assert state["user_blockers"]["work_order:strategy-user-1"]["active"] is True
        assert "licensed dataset" in state["messages"][-1]["text"]

    asyncio.run(scenario())


def test_backtest_retry_receives_review_blockers_and_requires_matching_market() -> None:
    worker_instructions = CodexWorkOrderRunner._developer_instructions("backtest")
    assert "never use Spot candles as a proxy for USD-M" in worker_instructions
    assert "binance-spot-btcusdt-4h-v1.csv" not in worker_instructions

    prompt = CodexWorkOrderRunner._prompt(
        {
            "id": "strategy-backtest-retry-1",
            "instruction": "Backtest a Binance USD-M BTCUSDT 4h strategy",
            "results": {
                "dataset": (
                    '{"schema":"chain-data-service-handoff/v1",'
                    '"datasets":[{"key":{"market":"usd-m"}}]}'
                ),
                "review_backtest": (
                    "# FAIL\nBLOCKER: Spot candles cannot validate USD-M perpetual signals"
                )
            },
        },
        role="backtest",
        prior_result="Strategy implementation handoff",
    )
    assert "Strategy implementation handoff" in prompt
    assert "Deterministic Data Service handoff" in prompt
    assert '"market":"usd-m"' in prompt
    assert "Independent Review Agent findings from the prior backtest" in prompt
    assert "Spot candles cannot validate USD-M perpetual signals" in prompt


def test_review_verdict_requires_explicit_first_line_pass() -> None:
    assert CodexWorkOrderRunner._review_passed("# PASS\nall gates verified")
    assert CodexWorkOrderRunner._review_passed("**PASS — ready for implementation approval.**")
    assert not CodexWorkOrderRunner._review_passed("# FAIL\nnot ready")
    assert not CodexWorkOrderRunner._review_passed("PASSIVE checks are incomplete")
    assert not CodexWorkOrderRunner._review_passed("Evidence looks good")


def test_review_report_requires_full_stage_coverage_and_blocking_evidence() -> None:
    outcome = CodexWorkOrderRunner._review_outcome
    implementation = "review_implementation"
    backtest = "review_backtest"
    assert outcome(_review_report(implementation), implementation) == "pass"
    assert outcome(
        _review_report(implementation, "FAIL", ("missing edge-case test",)),
        implementation,
    ) == "fail"
    assert outcome(_review_report(backtest), backtest) == "pass"
    assert outcome(_review_report(implementation), backtest) == "incomplete"
    assert outcome("# PASS\nREVIEW_STAGE: review_implementation", implementation) == "incomplete"
    assert outcome("# FAIL\nREVIEW_STAGE: review_implementation", implementation) == "incomplete"
    assert outcome(
        _review_report(implementation).replace("CHECK: safety | verified in test fixture", ""),
        implementation,
    ) == "incomplete"
    assert outcome(
        _review_report(implementation, "PASS", ("contradictory blocker",)),
        implementation,
    ) == "incomplete"
    assert outcome(
        "# BLOCKED\nREVIEW_STAGE: review_implementation\n"
        "PREREQUISITE_BLOCKER: versioned artifact is missing; Strategy must create it",
        implementation,
    ) == "blocked"


def test_incomplete_review_stays_with_review_before_one_batched_strategy_return(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def fake_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del prior_result
            calls.append(role)
            if role == "strategy":
                return "built"
            assert order["stage"] == "review_implementation"
            if calls.count("review") == 1:
                return (
                    "# FAIL\nREVIEW_STAGE: review_implementation\n"
                    "CHECK: spec | checked\nBLOCKER: first issue"
                )
            return _review_report(
                "review_implementation",
                "FAIL",
                ("first issue", "second issue"),
            )

        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "signal-batched-review-1",
            "agent": "strategy",
            "artifact_kind": "signal",
            "backtest_required": False,
            "status": "working",
            "stage": "strategy",
            "instruction": "add a tested neutral signal",
        }
        await store.write({"messages": [], "work_orders": [order]})
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=fake_turn)

        await worker._execute(order)
        pending = (await store.read())["work_orders"][0]
        assert calls == ["strategy", "review"]
        assert pending["status"] == "queued"
        assert pending["stage"] == "review_implementation"
        assert pending["attempts"] == 1
        assert "incomplete" in pending["last_error"]
        review_prompt = worker._prompt(
            pending, role="review", prior_result=pending["results"]["strategy"]
        )
        assert "Prior Review report" in review_prompt
        assert "CHECK: <category>" in review_prompt

        await worker._execute(pending)
        returned = (await store.read())["work_orders"][0]
        assert calls == ["strategy", "review", "review"]
        assert returned["status"] == "queued"
        assert returned["stage"] == "strategy"
        assert returned["attempts"] == 2
        assert returned["results"]["review_implementation"].count("BLOCKER:") == 2

    asyncio.run(scenario())


def test_incomplete_backtest_review_exhausts_review_budget_without_backtest_rework(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        calls: list[str] = []

        async def fake_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del prior_result
            calls.append(role)
            assert role == "review"
            assert order["stage"] == "review_backtest"
            return "# FAIL\nREVIEW_STAGE: review_backtest\nBLOCKER: missing chart"

        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "strategy-partial-backtest-review-1",
            "agent": "strategy",
            "status": "working",
            "stage": "review_backtest",
            "results": {"strategy": "built", "backtest": "report available"},
            "instruction": "backtest the strategy",
        }
        await store.write({"messages": [], "work_orders": [order]})
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=fake_turn,
            max_attempts=2,
        )
        await worker._execute(order)
        pending = (await store.read())["work_orders"][0]
        assert pending["status"] == "queued"
        assert pending["stage"] == "review_backtest"

        await worker._execute(pending)
        exhausted = (await store.read())["work_orders"][0]
        assert calls == ["review", "review"]
        assert exhausted["status"] == "dead_letter"
        assert exhausted["stage"] == "review_backtest"
        assert exhausted["attempts"] == 2

    asyncio.run(scenario())


def test_blocked_review_returns_missing_prerequisite_to_author(tmp_path: Path) -> None:
    async def scenario() -> None:
        async def fake_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del prior_result
            assert role == "review"
            assert order["stage"] == "review_implementation"
            return (
                "# BLOCKED\nREVIEW_STAGE: review_implementation\n"
                "PREREQUISITE_BLOCKER: source artifact is missing; Strategy must create it"
            )

        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "signal-prerequisite-1",
            "agent": "strategy",
            "status": "working",
            "stage": "review_implementation",
            "results": {"strategy": "claimed implementation complete"},
            "instruction": "add signal",
        }
        await store.write({"messages": [], "work_orders": [order]})
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=fake_turn)
        await worker._execute(order)
        returned = (await store.read())["work_orders"][0]
        assert returned["status"] == "queued"
        assert returned["stage"] == "strategy"
        assert "BLOCKED" in returned["last_error"]

    asyncio.run(scenario())


def test_worker_recovers_markdown_pass_misclassified_as_dead_letter(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "dead_letters": [{"id": "signal-review-pass-1", "reason": "false fail"}],
                "work_orders": [
                    {
                        "id": "signal-review-pass-1",
                        "agent": "strategy",
                        "status": "dead_letter",
                        "stage": "strategy",
                        "attempts": 5,
                        "last_error": "Review Agent returned FAIL for implementation evidence",
                        "approvals": {"implementation": None, "backtest": None, "live": None},
                        "results": {
                            "strategy": "built",
                            "review_implementation": "**PASS — implementation verified.**",
                        },
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)

        await worker._recover_passed_review_dead_letters()

        state = await store.read()
        order = state["work_orders"][0]
        assert order["status"] == "awaiting_implementation_approval"
        assert order["stage"] == "implementation_approval"
        assert order["attempts"] == 5
        assert order["approvals"]["implementation"] is None
        assert "last_error" not in order
        assert state["dead_letters"] == []
        assert state["messages"][-1]["author"] == "MANAGER"

    asyncio.run(scenario())


def test_worker_recovers_legacy_failed_review_approval_state(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-legacy-fail-1",
                        "agent": "strategy",
                        "status": "awaiting_implementation_approval",
                        "stage": "implementation_approval",
                        "results": {
                            "strategy": "built",
                            "review_implementation": "# FAIL\nmissing boundary test",
                        },
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)

        await worker._recover_failed_review_orders()

        state = await store.read()
        order = state["work_orders"][0]
        assert order["status"] == "queued"
        assert order["stage"] == "strategy"
        assert order["attempts"] == 1
        assert order["results"]["review_implementation"].startswith("# FAIL")
        assert state["messages"][-1]["author"] == "MANAGER"

    asyncio.run(scenario())


def test_codex_worker_requeues_timeout_without_losing_stage(tmp_path: Path) -> None:
    async def scenario() -> None:
        async def slow_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            await asyncio.sleep(1)
            return "unreachable"

        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "strategy-timeout-1",
            "agent": "strategy",
            "status": "working",
            "stage": "strategy",
            "results": {},
        }
        await store.write({"messages": [], "work_orders": [order]})
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=slow_turn,
            turn_timeout_seconds=0.01,
        )

        await worker._execute(order)

        state = await store.read()
        saved = state["work_orders"][0]
        assert saved["status"] == "queued"
        assert saved["stage"] == "strategy"
        assert saved["attempts"] == 1
        assert "exceeded" in saved["last_error"]

    asyncio.run(scenario())


def test_codex_worker_claim_preserves_optimize_stage(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-optimize-1",
                        "agent": "strategy",
                        "status": "queued",
                        "stage": "optimize",
                        "results": {
                            "backtest": "complete",
                            "review_backtest": _review_report("review_backtest"),
                        },
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)

        claimed = await worker._claim_next()

        assert claimed is not None
        assert claimed["stage"] == "optimize"
        state = await store.read()
        assert state["work_orders"][0]["stage"] == "optimize"
        assert state["messages"][-1]["author"] == "OPTIMIZE"

    asyncio.run(scenario())


def test_worker_recovers_optimizer_stage_regression_dead_letter(tmp_path: Path) -> None:
    async def scenario() -> None:
        reason = "Codex strategy turn exceeded 3600 seconds"
        issue_key = CodexWorkOrderRunner._issue_key("strategy", reason)
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "dead_letters": [
                    {"id": "strategy-optimize-recovery-1", "reason": reason}
                ],
                "work_orders": [
                    {
                        "id": "strategy-optimize-recovery-1",
                        "agent": "strategy",
                        "status": "dead_letter",
                        "stage": "strategy",
                        "optimization_required": True,
                        "attempts": 9,
                        "issue_counts": {issue_key: 6, "review_backtest:prior": 3},
                        "last_error": reason,
                        "recurring_issues": [issue_key],
                        "finished_at": "2026-01-01T00:00:00+00:00",
                        "results": {
                            "strategy": "implemented",
                            "backtest": "complete",
                            "review_backtest": _review_report("review_backtest"),
                        },
                        "checkpoints": [
                            {"stage": "optimize", "status": "working"},
                            {"stage": "strategy", "status": "working"},
                        ],
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)

        await worker._recover_optimizer_stage_regressions()

        state = await store.read()
        order = state["work_orders"][0]
        assert order["status"] == "queued"
        assert order["stage"] == "optimize"
        assert order["attempts"] == 3
        assert order["issue_counts"] == {"review_backtest:prior": 3}
        assert "last_error" not in order
        assert "recurring_issues" not in order
        assert order["results"]["backtest"] == "complete"
        assert order["checkpoints"][-1]["status"] == "queued_after_stage_recovery"
        assert state["dead_letters"] == []
        assert state["messages"][-1]["author"] == "MAINTENANCE"

    asyncio.run(scenario())


def test_codex_worker_backs_off_transient_usage_limit_without_losing_stage(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async def limited_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            raise RuntimeError("You've hit your usage limit. Try again at 8:21 AM.")

        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "strategy-limited-1",
            "agent": "strategy",
            "status": "working",
            "stage": "strategy",
            "results": {},
        }
        await store.write({"messages": [], "work_orders": [order]})
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=limited_turn,
        )

        await worker._execute(order)

        state = await store.read()
        saved = state["work_orders"][0]
        assert saved["status"] == "queued"
        assert saved["stage"] == "strategy"
        assert saved.get("attempts", 0) == 0
        assert datetime.fromisoformat(saved["retry_not_before"]) > datetime.now(UTC)
        assert "usage limit" in saved["last_error"]
        assert await worker._claim_next() is None

    asyncio.run(scenario())


def test_codex_worker_claim_clears_stale_retry_error(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-retry-ready-1",
                        "agent": "strategy",
                        "status": "queued",
                        "stage": "review_implementation",
                        "last_error": "RuntimeError: usage limit",
                        "results": {"strategy": "built"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=lambda *_: None,  # type: ignore[arg-type]
        )

        claimed = await worker._claim_next()

        assert claimed is not None
        assert "last_error" not in claimed
        assert "last_error" not in (await store.read())["work_orders"][0]

    asyncio.run(scenario())


def test_codex_worker_resumes_backtest_from_persisted_strategy_handoff(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        calls: list[tuple[str, str | None]] = []

        async def fake_dataset(order: dict[str, Any]) -> str:
            assert order["id"] == "strategy-resume-1"
            return '{"schema":"chain-data-service-handoff/v1","datasets":[]}'

        async def fake_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            calls.append((role, prior_result))
            return (
                _review_report(order["stage"])
                if role == "review"
                else "backtest-complete"
            )

        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "strategy-resume-1",
            "agent": "strategy",
            "status": "working",
            "stage": "backtest",
            "results": {"strategy": "strategy-complete"},
        }
        await store.write({"messages": [], "work_orders": [order]})
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=fake_turn,
            dataset_materializer=fake_dataset,
        )

        await worker._execute(order)

        state = await store.read()
        saved = state["work_orders"][0]
        assert calls == [
            ("backtest", "strategy-complete"),
            ("review", "backtest-complete"),
        ]
        assert saved["status"] == "awaiting_backtest_approval"
        assert "chain-data-service-handoff/v1" in saved["results"]["dataset"]
        assert saved["results"]["backtest"] == "backtest-complete"

    asyncio.run(scenario())


def test_codex_worker_can_cancel_the_only_active_work_order(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        entered = asyncio.Event()

        async def blocking_turn(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            entered.set()
            await asyncio.Event().wait()
            return "unreachable"

        store = JsonStateStore(tmp_path / "state.json")
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=blocking_turn)
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            worker=worker,
        )
        queued = await service.handle_manager_message("新增均線策略")
        identifier = queued["snapshot"]["work_orders"][0]["id"]
        worker.start()
        await asyncio.wait_for(entered.wait(), timeout=1)
        assert await worker.cancel() == identifier
        snapshot = await service.snapshot()
        assert snapshot["work_orders"][0]["status"] == "cancelled"
        await worker.close()

    asyncio.run(scenario())


def test_codex_worker_keeps_orders_queued_when_authentication_is_missing(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        async def should_not_run(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            raise AssertionError("agent turn must not start without authentication")

        async def not_ready() -> tuple[bool, str]:
            return False, "Codex login required"

        store = JsonStateStore(tmp_path / "state.json")
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=should_not_run,
            readiness_probe=not_ready,
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            worker=worker,
        )
        await service.handle_manager_message("新增均線策略")
        worker.start()
        await asyncio.sleep(0.01)
        snapshot = await service.snapshot()
        assert snapshot["work_orders"][0]["status"] == "queued"
        assert snapshot["agents"]["strategy"]["status"] == "BLOCKED"
        assert snapshot["agents"]["strategy"]["task"] == "Codex login required"
        assert snapshot["user_blockers"][0]["code"] == "codex_readiness"
        assert snapshot["user_blockers"][0]["detail"] == "Codex login required"
        notices = [
            item
            for item in snapshot["messages"]
            if item["author"] == "MANAGER" and "需要使用者處理" in item["text"]
        ]
        assert len(notices) == 1
        await worker.close()

    asyncio.run(scenario())


def test_codex_worker_recovers_interrupted_order_before_auth_probe(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async def should_not_run(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            raise AssertionError("agent turn must not start")

        async def not_ready() -> tuple[bool, str]:
            return False, "Codex login required"

        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-interrupted-1",
                        "agent": "strategy",
                        "status": "working",
                        "stage": "strategy",
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=should_not_run,
            readiness_probe=not_ready,
        )
        worker.start()
        await asyncio.sleep(0.01)
        state = await store.read()
        assert state["work_orders"][0]["status"] == "queued"
        assert state["work_orders"][0]["stage"] == "strategy"
        assert "安全退回佇列" in state["messages"][0]["text"]
        await worker.close()

    asyncio.run(scenario())


def test_codex_worker_recovers_complete_local_backtest_during_usage_limit(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        calls = 0

        async def should_not_run(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            nonlocal calls
            del order, role, prior_result
            calls += 1
            raise AssertionError("complete local evidence must bypass Codex")

        async def not_ready() -> tuple[bool, str]:
            return False, "Codex usage limited"

        reports = tmp_path / "reports"
        charts = reports / "charts" / "fixture"
        charts.mkdir(parents=True)
        chart_names = (
            "cagr_roi",
            "max_drawdown",
            "sharpe_ratio",
            "win_rate_payoff_ratio",
            "trade_count",
        )
        for chart_name in chart_names:
            (charts / f"{chart_name}.svg").write_text("<svg/>", encoding="utf-8")
        manifest = reports / "fixture.json"
        manifest.write_text(
            json.dumps(
                {
                    "work_order": "strategy-recover-evidence-1",
                    "status": "complete",
                    "research_only": True,
                    "auto_promote": False,
                    "approval": {"required": True},
                    "provenance": {
                        "candle_count": 123,
                        "normalized_dataset_sha256": "a" * 64,
                    },
                    "final_test": {
                        "net_return": 0.01,
                        "max_drawdown": -0.02,
                        "trade_count": 3,
                    },
                    "performance_standard": {
                        "initial_equity_usd": 10000,
                        "cagr": 0.01,
                        "roi": 0.01,
                        "max_drawdown": -0.02,
                        "sharpe_ratio": 0.4,
                        "win_rate": 0.5,
                        "payoff_ratio": 1.1,
                        "trade_count": 3,
                    },
                    "charts": {name: f"reports/charts/fixture/{name}.svg" for name in chart_names},
                }
            ),
            encoding="utf-8",
        )
        manifest.with_suffix(".md").write_text("# Evidence", encoding="utf-8")
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-recover-evidence-1",
                        "agent": "strategy",
                        "status": "queued",
                        "stage": "backtest",
                        "last_error": "RuntimeError: You've hit your usage limit.",
                        "retry_not_before": "2099-01-01T00:00:00+00:00",
                        "results": {"strategy": "strategy-complete"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=should_not_run,
            readiness_probe=not_ready,
        )

        worker.start()
        for _ in range(100):
            saved = (await store.read())["work_orders"][0]
            if saved["status"] == "awaiting_backtest_approval":
                break
            await asyncio.sleep(0.01)
        await worker.close()

        assert calls == 0
        assert saved["stage"] == "backtest_approval"
        assert "retry_not_before" not in saved
        assert "Native Backtest recovery gate" in saved["results"]["backtest"]
        assert (tmp_path / "work/studio/strategy-recover-evidence-1/result.json").is_file()

    asyncio.run(scenario())


def test_codex_worker_does_not_recover_incomplete_local_backtest(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async def should_not_run(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            raise AssertionError("authentication probe should stop the worker")

        async def not_ready() -> tuple[bool, str]:
            return False, "Codex usage limited"

        reports = tmp_path / "reports"
        reports.mkdir()
        (reports / "fixture.json").write_text(
            json.dumps(
                {
                    "work_order": "strategy-incomplete-evidence-1",
                    "status": "complete",
                    "research_only": True,
                    "auto_promote": False,
                    "approval": {"required": True},
                    "provenance": {
                        "candle_count": 123,
                        "normalized_dataset_sha256": "a" * 64,
                    },
                    "final_test": {
                        "net_return": 0.01,
                        "max_drawdown": -0.02,
                        "trade_count": 3,
                    },
                    "charts": {"equity": "reports/charts/missing.svg"},
                }
            ),
            encoding="utf-8",
        )
        (reports / "fixture.md").write_text("# Evidence", encoding="utf-8")
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-incomplete-evidence-1",
                        "agent": "strategy",
                        "status": "queued",
                        "stage": "backtest",
                        "last_error": "RuntimeError: You've hit your usage limit.",
                        "results": {"strategy": "strategy-complete"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=should_not_run,
            readiness_probe=not_ready,
        )

        worker.start()
        await asyncio.sleep(0.02)
        await worker.close()

        saved = (await store.read())["work_orders"][0]
        assert saved["status"] == "queued"
        assert "backtest" not in saved["results"]

    asyncio.run(scenario())


def test_codex_worker_clears_stale_retry_metadata_from_terminal_orders(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        async def should_not_run(order: dict[str, Any], role: str, prior_result: str | None) -> str:
            del order, role, prior_result
            raise AssertionError("terminal work orders must not execute")

        async def not_ready() -> tuple[bool, str]:
            return False, "Codex unavailable"

        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-reviewed-1",
                        "agent": "strategy",
                        "status": "awaiting_review",
                        "stage": "user_review",
                        "retry_not_before": "2099-01-01T00:00:00+00:00",
                        "results": {"strategy": "done", "backtest": "done"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=should_not_run,
            readiness_probe=not_ready,
        )

        worker.start()
        for _ in range(100):
            saved = (await store.read())["work_orders"][0]
            if "retry_not_before" not in saved:
                break
            await asyncio.sleep(0.01)
        await worker.close()

        assert saved["status"] == "awaiting_review"
        assert saved["stage"] == "user_review"
        assert "retry_not_before" not in saved

    asyncio.run(scenario())


def test_http_gateway_requires_exact_origin_and_client_header(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        server = TestServer(create_app(service=service, allowed_origins=("https://studio.test",)))
        client = TestClient(server)
        await client.start_server()
        try:
            denied_origin = await client.get(
                "/api/v1/studio", headers={"Origin": "https://attacker.test"}
            )
            assert denied_origin.status == 403

            missing_header = await client.post(
                "/api/v1/manager/message",
                json={"message": "狀態報告"},
                headers={"Origin": "https://studio.test"},
            )
            assert missing_header.status == 403

            accepted = await client.post(
                "/api/v1/manager/message",
                json={"message": "狀態報告"},
                headers={
                    "Origin": "https://studio.test",
                    "X-Chain-Client": "quant-studio-v1",
                },
            )
            assert accepted.status == 200
            assert accepted.headers["Access-Control-Allow-Origin"] == "https://studio.test"
            payload = await accepted.json()
            assert payload["snapshot"]["trading_enabled"] is False
        finally:
            await client.close()

    asyncio.run(scenario())


def test_http_gateway_reports_codex_readiness_without_credentials(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    class FakeWorker:
        def start(self) -> None:
            return None

        async def close(self) -> None:
            return None

        async def readiness(self) -> tuple[bool, str]:
            return True, "Codex authenticated"

    class FakeLoginProcess:
        def __init__(self) -> None:
            self.returncode: int | None = None

        def poll(self) -> int | None:
            return self.returncode

        def wait(self, timeout: float | None = None) -> int:
            del timeout
            self.returncode = 0
            return 0

        def terminate(self) -> None:
            self.returncode = 0

    launches: list[FakeLoginProcess] = []

    def launch_login() -> Any:
        process = FakeLoginProcess()
        launches.append(process)
        return process

    async def scenario() -> None:
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            worker=FakeWorker(),  # type: ignore[arg-type]
            codex_login_launcher=launch_login,
        )
        server = TestServer(create_app(service=service, allowed_origins=("https://studio.test",)))
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get(
                "/api/v1/codex/health",
                headers={"Origin": "https://studio.test"},
            )
            assert response.status == 200
            assert response.headers["Cache-Control"] == "no-store"
            payload = await response.json()
            assert payload["ok"] is True
            assert payload["ready"] is True
            assert payload["reason"] == "Codex authenticated"
            assert payload["login_supported"] is True
            assert payload["login_in_progress"] is False
            serialized = json.dumps(payload).lower()
            assert "access_token" not in serialized
            assert "refresh_token" not in serialized
            assert "auth.json" not in serialized

            denied = await client.post(
                "/api/v1/codex/login",
                headers={"Origin": "https://studio.test"},
            )
            assert denied.status == 403
            started = await client.post(
                "/api/v1/codex/login",
                headers={
                    "Origin": "https://studio.test",
                    "X-Chain-Client": "quant-studio-v1",
                },
            )
            assert started.status == 200
            assert await started.json() == {
                "ok": True,
                "started": True,
                "state": "browser_login_started",
            }
            repeated = await client.post(
                "/api/v1/codex/login",
                headers={
                    "Origin": "https://studio.test",
                    "X-Chain-Client": "quant-studio-v1",
                },
            )
            assert (await repeated.json())["state"] == "already_running"
            assert len(launches) == 1
        finally:
            await client.close()

    asyncio.run(scenario())


def test_http_gateway_uploads_spec_and_enqueues_explicit_artifact(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        server = TestServer(create_app(service=service, allowed_origins=("https://studio.test",)))
        client = TestClient(server)
        await client.start_server()
        try:
            form = FormData()
            form.add_field(
                "spec",
                b"# BTC 4h EMA\n\nArtifact type: `strategy`\n",
                filename="spec.md",
                content_type="text/markdown",
            )
            form.add_field("artifact_kind", "strategy")
            form.add_field("backtest_required", "false")
            form.add_field("note", "Use public Binance data only")
            accepted = await client.post(
                "/api/v1/manager/spec",
                data=form,
                headers={
                    "Origin": "https://studio.test",
                    "X-Chain-Client": "quant-studio-v1",
                },
            )
            assert accepted.status == 200
            payload = await accepted.json()
            assert payload["action"] == "add_strategy"
            assert payload["uploaded_spec"]["name"] == "spec.md"
            spec_path = tmp_path / payload["uploaded_spec"]["path"]
            assert spec_path.read_text(encoding="utf-8").startswith("# BTC 4h EMA")
            state = await store.read()
            order = state["work_orders"][0]
            assert order["artifact_kind"] == "strategy"
            assert order["backtest_required"] is False
            assert [node["node_id"] for node in order["dag"]["nodes"]] == [
                "strategy",
                "review_implementation",
                "implementation_approval",
                "live_approval",
            ]
            assert payload["uploaded_spec"]["path"] in order["instruction"]
            assert payload["uploaded_spec"]["backtest_required"] is False
            assert "不需要回測" in order["instruction"]
            assert len(payload["snapshot"]["artifact_registry"]) == 1
            assert payload["snapshot"]["artifact_registry"][0]["spec_sha256"]

            invalid = FormData()
            invalid.add_field(
                "spec",
                b"# Wrong name",
                filename="strategy.md",
                content_type="text/markdown",
            )
            invalid.add_field("artifact_kind", "strategy")
            invalid.add_field("backtest_required", "true")
            rejected = await client.post(
                "/api/v1/manager/spec",
                data=invalid,
                headers={
                    "Origin": "https://studio.test",
                    "X-Chain-Client": "quant-studio-v1",
                },
            )
            assert rejected.status == 400
            assert "named spec.md" in await rejected.text()
        finally:
            await client.close()

    asyncio.run(scenario())


def test_artifact_record_resolves_internal_spec_and_hashes_referenced_worktree(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        pass

    head = "a" * 40
    ref = tmp_path / ".git" / "refs" / "heads" / "main"
    ref.parent.mkdir(parents=True)
    (tmp_path / ".git" / "HEAD").write_text("ref: refs/heads/main\n", encoding="utf-8")
    ref.write_text(head + "\n", encoding="utf-8")
    artifact = tmp_path / "signals" / "implemented" / "sample-signal"
    artifact.mkdir(parents=True)
    spec = artifact / "spec.md"
    spec.write_text("# normalized EMA spec\n", encoding="utf-8")
    (artifact / "1.0.0.toml").write_text(
        'stable_id = "sample-signal"\n'
        'version = "1.0.0"\n'
        'work_order = "sample-signal"\n'
        'tests = ["tests/test_ema.py"]\n',
        encoding="utf-8",
    )
    source = tmp_path / "src" / "quant_signal_agent" / "signals" / "ema.py"
    source.parent.mkdir(parents=True)
    source.write_text("VERSION = 1\n", encoding="utf-8")
    test = tmp_path / "tests" / "test_ema.py"
    test.parent.mkdir()
    test.write_text("def test_version(): pass\n", encoding="utf-8")
    worker = CodexWorkOrderRunner(
        project_root=tmp_path,
        store=JsonStateStore(tmp_path / "state.json"),
        agent_turn=lambda *_args, **_kwargs: None,  # type: ignore[arg-type]
    )

    worker._record_artifact(
        {
            "id": "sample-signal",
            "artifact_kind": "signal",
            "instruction": "create an EMA signal",
            "results": {
                "strategy": (
                    "src/quant_signal_agent/signals/ema.py "
                    "tests/test_ema.py signals/implemented/sample-signal/spec.md"
                )
            },
        }
    )

    record = worker.artifact_registry.list_records()[0]
    assert record["spec_sha256"] == hashlib.sha256(spec.read_bytes()).hexdigest()
    assert record["code_revision"].startswith(head + "+worktree.")
    assert record["tests"] == ["tests/test_ema.py"]


def test_artifact_record_requires_exact_version_when_work_order_has_two_manifests(
    tmp_path: Path,
) -> None:
    artifact = tmp_path / "signals" / "implemented" / "sample-signal"
    artifact.mkdir(parents=True)
    (artifact / "spec.md").write_text("# shared user spec\n", encoding="utf-8")
    (artifact / "normalized-spec-1.1.0.json").write_text(
        '{"artifact":{"version":"1.1.0"}}\n', encoding="utf-8"
    )
    for version, test in (("1.0.0", "test_old.py"), ("1.1.0", "test_new.py")):
        (artifact / f"{version}.toml").write_text(
            f'stable_id = "sample-signal"\nversion = "{version}"\n'
            f'work_order = "sample-signal"\ntests = ["tests/{test}"]\n',
            encoding="utf-8",
        )
    worker = CodexWorkOrderRunner(
        project_root=tmp_path,
        store=JsonStateStore(tmp_path / "state.json"),
        agent_turn=lambda *_args, **_kwargs: None,  # type: ignore[arg-type]
    )
    order = {"id": "sample-signal", "artifact_kind": "signal", "results": {}}
    with pytest.raises(ValueError, match="multiple manifests"):
        worker._record_artifact(order)
    assert worker.artifact_registry.list_records() == []

    worker._record_artifact({**order, "artifact_id": "sample-signal",
                             "artifact_version": "1.1.0"})
    record = worker.artifact_registry.list_records()[0]
    assert record["spec_sha256"] == hashlib.sha256(
        (artifact / "spec.md").read_bytes()
    ).hexdigest()
    assert record["tests"] == ["tests/test_new.py"]
    assert record["artifact_version"] == "1.1.0"
    assert record["manifest_sha256"] == hashlib.sha256(
        (artifact / "1.1.0.toml").read_bytes()
    ).hexdigest()

    with pytest.raises(ValueError, match="exact work-order artifact version"):
        worker._record_artifact({**order, "artifact_version": "1.2.0"})


def test_strategy_handoff_binds_exact_artifact_version_from_evidence(tmp_path: Path) -> None:
    artifact = tmp_path / "signals" / "implemented" / "sample-signal"
    artifact.mkdir(parents=True)
    for version in ("1.0.0", "1.1.0"):
        (artifact / f"{version}.toml").write_text(
            f'stable_id = "sample-signal"\nversion = "{version}"\n'
            'work_order = "sample-signal"\n',
            encoding="utf-8",
        )
    worker = CodexWorkOrderRunner(
        project_root=tmp_path,
        store=JsonStateStore(tmp_path / "state.json"),
        agent_turn=lambda *_args, **_kwargs: None,  # type: ignore[arg-type]
    )
    order: dict[str, Any] = {"id": "sample-signal", "artifact_kind": "signal"}

    worker._bind_artifact_identity_from_evidence(
        order,
        "Review `signals/implemented/sample-signal/1.1.0.toml` as "
        "`sample-signal@1.1.0`.",
    )

    assert order["artifact_id"] == "sample-signal"
    assert order["artifact_version"] == "1.1.0"


def test_http_gateway_enforces_three_explicit_approval_gates(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "signal-gates-1",
                        "agent": "strategy",
                        "artifact_kind": "signal",
                        "backtest_required": False,
                        "status": "awaiting_implementation_approval",
                        "stage": "implementation_approval",
                        "approvals": {
                            "implementation": None,
                            "backtest": None,
                            "live": None,
                        },
                        "results": {
                            "strategy": "complete",
                            "review_implementation": "PASS",
                        },
                    }
                ],
            }
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        server = TestServer(create_app(service=service))
        client = TestClient(server)
        await client.start_server()
        headers = {"X-Chain-Client": "quant-studio-v1"}
        try:
            implementation = await client.post(
                "/api/v1/work-orders/signal-gates-1/approval",
                json={
                    "gate": "implementation_approval",
                    "approved": True,
                    "note": "implementation accepted",
                },
                headers=headers,
            )
            assert implementation.status == 200
            first = (await implementation.json())["snapshot"]["work_orders"][0]
            assert first["status"] == "awaiting_live_approval"
            assert first["approvals"]["backtest"] is None

            wrong_gate = await client.post(
                "/api/v1/work-orders/signal-gates-1/approval",
                json={"gate": "backtest_approval", "approved": True, "note": "wrong"},
                headers=headers,
            )
            assert wrong_gate.status == 400

            live = await client.post(
                "/api/v1/work-orders/signal-gates-1/approval",
                json={"gate": "live_approval", "approved": True, "note": "release"},
                headers=headers,
            )
            assert live.status == 200
            final = (await live.json())["snapshot"]["work_orders"][0]
            assert final["status"] == "approved"
            assert final["stage"] == "ready_for_signal"
        finally:
            await client.close()

    asyncio.run(scenario())


def test_codex_worker_opens_circuit_without_consuming_retry_budget(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        order = {
            "id": "strategy-dead-letter-1",
            "agent": "strategy",
            "status": "working",
            "stage": "strategy",
            "attempts": 1,
            "results": {},
        }
        await store.write({"messages": [], "work_orders": [order]})

        async def limited(*_: Any) -> str:
            raise RuntimeError("usage limit; try again at 11:59 PM")

        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=limited,
            max_attempts=2,
        )
        await worker._execute(order)

        state = await store.read()
        updated = state["work_orders"][0]
        assert updated["status"] == "queued"
        assert updated["attempts"] == 1
        assert updated["retry_not_before"]
        assert state.get("dead_letters", []) == []
        assert state["codex_circuit"]["state"] == "open"
        circuit_until = datetime.fromisoformat(state["codex_circuit"]["open_until"])
        order_until = datetime.fromisoformat(updated["retry_not_before"])
        assert abs((circuit_until - order_until).total_seconds()) < 1

    asyncio.run(scenario())


def test_dead_letter_can_retry_after_exact_version_remediation(tmp_path: Path) -> None:
    async def scenario() -> None:
        artifact = tmp_path / "signals" / "implemented" / "sample-signal"
        artifact.mkdir(parents=True)
        (artifact / "1.1.0.toml").write_text(
            'stable_id = "sample-signal"\n'
            'version = "1.1.0"\n'
            'work_order = "signal-dead-1"\n',
            encoding="utf-8",
        )
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "dead_letters": [{"id": "signal-dead-1", "reason": "review failed"}],
                "work_orders": [
                    {
                        "id": "signal-dead-1",
                        "agent": "strategy",
                        "artifact_kind": "signal",
                        "status": "dead_letter",
                        "stage": "strategy",
                        "attempts": 5,
                        "last_error": "review failed",
                        "results": {"review_implementation": "FAIL"},
                    }
                ],
            }
        )
        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=lambda *_args, **_kwargs: None,  # type: ignore[arg-type]
        )

        retried = await worker.retry_dead_letter_after_remediation(
            "signal-dead-1",
            artifact_id="sample-signal",
            artifact_version="1.1.0",
            reason="fixed review findings",
        )

        assert retried["status"] == "queued"
        assert retried["attempts"] == 0
        assert retried["artifact_id"] == "sample-signal"
        assert retried["artifact_version"] == "1.1.0"
        assert retried["checkpoints"][-1]["status"] == "queued_after_remediation"
        state = await store.read()
        assert state["dead_letters"] == []

    asyncio.run(scenario())


def test_remediated_backtest_resumes_without_rerunning_approved_strategy(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        artifact = tmp_path / "strategies" / "implemented" / "btc-research"
        artifact.mkdir(parents=True)
        (artifact / "1.0.0.toml").write_text(
            'stable_id = "btc-research"\nversion = "1.0.0"\n'
            'work_order = "strategy-dead-1"\n',
            encoding="utf-8",
        )
        store = JsonStateStore(tmp_path / "state.json")
        await store.write({
            "messages": [],
            "dead_letters": [{"id": "strategy-dead-1"}],
            "user_blockers": {"work_order:strategy-dead-1": {"active": True}},
            "work_orders": [{
                "id": "strategy-dead-1", "agent": "strategy",
                "artifact_kind": "strategy", "status": "dead_letter",
                "stage": "backtest", "attempts": 5,
                "issue_counts": {"review_backtest:wrong-venue": 2},
                "artifact_id": "btc-research", "artifact_version": "1.0.0",
                "approvals": {"implementation": {"approved": True}},
                "results": {
                    "strategy": "approved implementation",
                    "review_implementation": "# PASS",
                    "backtest": "wrong dataset",
                    "review_backtest": "# FAIL",
                },
            }],
        })
        worker = CodexWorkOrderRunner(project_root=tmp_path, store=store, agent_turn=None)
        resumed = await worker.retry_dead_letter_after_remediation(
            "strategy-dead-1", artifact_id="btc-research",
            artifact_version="1.0.0", reason="USD-M data and report fixed",
        )
        assert resumed["status"] == "queued"
        assert resumed["stage"] == "backtest"
        assert resumed["attempts"] == 0
        assert resumed["issue_counts"] == {}
        assert resumed["approvals"]["implementation"]["approved"] is True
        assert resumed["results"] == {
            "strategy": "approved implementation", "review_implementation": "# PASS"
        }
        state = await store.read()
        assert state["dead_letters"] == []
        assert state["user_blockers"]["work_order:strategy-dead-1"]["active"] is False

    asyncio.run(scenario())


def test_manager_can_resume_quota_blocked_codex_work_after_explicit_reset(
    tmp_path: Path,
) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "codex_circuit": {
                    "state": "open",
                    "open_until": "2099-01-01T00:00:00+00:00",
                    "reason": "usage limit",
                },
                "work_orders": [
                    {
                        "id": "signal-quota-1",
                        "agent": "strategy",
                        "status": "queued",
                        "stage": "review_implementation",
                        "attempts": 2,
                        "last_error": "RuntimeError: usage limit",
                        "retry_not_before": "2099-01-01T00:00:00+00:00",
                        "results": {"strategy": "built"},
                    }
                ],
            }
        )

        async def unavailable() -> tuple[bool, str]:
            return False, "test holds worker"

        worker = CodexWorkOrderRunner(
            project_root=tmp_path,
            store=store,
            agent_turn=lambda *_: None,  # type: ignore[arg-type]
            readiness_probe=unavailable,
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
            worker=worker,
        )

        result = await service.handle_manager_message("Codex 額度已重設，請恢復工作佇列")
        await worker.close()

        order = result["snapshot"]["work_orders"][0]
        assert result["action"] == "resume_codex_work"
        assert result["handoff"] == "review"
        assert order["status"] == "queued"
        assert order["stage"] == "review_implementation"
        assert order["attempts"] == 2
        assert "retry_not_before" not in order
        assert order["quota_retry_resumed_at"]
        assert result["snapshot"]["codex_circuit"]["state"] == "closed"

    asyncio.run(scenario())


def test_http_gateway_lists_and_serves_backtest_artifacts(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        reports = tmp_path / "reports"
        charts = reports / "charts" / "fixture"
        charts.mkdir(parents=True)
        (charts / "equity.svg").write_text("<svg></svg>", encoding="utf-8")
        (reports / "fixture.md").write_text("# Backtest evidence", encoding="utf-8")
        (reports / "fixture.json").write_text(
            json.dumps(
                {
                    "work_order": "strategy-report-1",
                    "status": "complete",
                    "strategy": {"stable_id": "btc-4h-fixture"},
                    "provenance": {"candle_count": 123},
                    "charts": {"equity": "reports/charts/fixture/equity.svg"},
                }
            ),
            encoding="utf-8",
        )
        blocked = reports / "backtests" / "btc-walk" / "dataset-gate"
        blocked.mkdir(parents=True)
        (blocked / "manifest.json").write_text(
            json.dumps(
                {
                    "work_order": "strategy-nested-1",
                    "status": "blocked_missing_compatible_dataset",
                    "engine": {"executed": False},
                }
            ),
            encoding="utf-8",
        )
        completed = reports / "backtests" / "btc-walk" / "completed"
        completed.mkdir(parents=True)
        (completed / "report.md").write_text("# Nested backtest evidence", encoding="utf-8")
        (completed / "equity.svg").write_text("<svg></svg>", encoding="utf-8")
        (completed / "manifest.json").write_text(
            json.dumps(
                {
                    "work_order": "strategy-nested-1",
                    "status": "complete_awaiting_independent_review",
                    "artifact": {"stable_id": "btc-walk"},
                    "engine": {"executed": True},
                    "performance_standard": {"cagr": 0.2},
                    "provenance": {"dataset": {"row_count": 14736}},
                    "evidence": {
                        "report": "reports/backtests/btc-walk/completed/report.md",
                        "charts": {
                            "equity": "reports/backtests/btc-walk/completed/equity.svg"
                        },
                    },
                }
            ),
            encoding="utf-8",
        )
        store = JsonStateStore(tmp_path / "state.json")
        await store.write(
            {
                "messages": [],
                "work_orders": [
                    {
                        "id": "strategy-report-1",
                        "agent": "strategy",
                        "status": "awaiting_review",
                        "stage": "user_review",
                        "results": {"strategy": "done", "backtest": "done"},
                    },
                    {
                        "id": "strategy-nested-1",
                        "agent": "strategy",
                        "status": "awaiting_review",
                        "stage": "user_review",
                        "results": {"strategy": "done", "backtest": "done"},
                    },
                ],
            }
        )
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        server = TestServer(create_app(service=service, allowed_origins=("https://studio.test",)))
        client = TestClient(server)
        await client.start_server()
        try:
            response = await client.get("/api/v1/studio", headers={"Origin": "https://studio.test"})
            payload = await response.json()
            backtest_file = payload["work_orders"][0]["backtest_file"]
            assert backtest_file["title"] == "btc-4h-fixture"
            assert backtest_file["candle_count"] == 123
            assert len(backtest_file["artifacts"]) == 3
            report = next(item for item in backtest_file["artifacts"] if item["kind"] == "report")
            opened = await client.get(report["url"], headers={"Origin": "https://studio.test"})
            assert opened.status == 200
            assert opened.content_type == "text/markdown"
            assert await opened.text() == "# Backtest evidence"
            assert opened.headers["Content-Disposition"] == 'inline; filename="fixture.md"'
            nested_file = payload["work_orders"][1]["backtest_file"]
            assert nested_file["title"] == "btc-walk"
            assert nested_file["candle_count"] == 14736
            assert len(nested_file["artifacts"]) == 3
            nested_report = next(
                item for item in nested_file["artifacts"] if item["kind"] == "report"
            )
            nested_chart = next(
                item for item in nested_file["artifacts"] if item["kind"] == "chart"
            )
            report_response = await client.get(
                nested_report["url"], headers={"Origin": "https://studio.test"}
            )
            assert report_response.status == 200
            assert await report_response.text() == "# Nested backtest evidence"
            chart_response = await client.get(
                nested_chart["url"], headers={"Origin": "https://studio.test"}
            )
            assert chart_response.status == 200
            assert await chart_response.text() == "<svg></svg>"
            try:
                service.resolve_report_artifact("AGENTS.md")
            except ValueError as exc:
                assert "inside reports" in str(exc)
            else:
                raise AssertionError("artifact traversal guard should reject project files")
        finally:
            await client.close()

    asyncio.run(scenario())


def test_manager_accepts_spec_with_text_and_image_context(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        store = JsonStateStore(tmp_path / "state.json")
        service = StudioService(
            project_root=tmp_path,
            store=store,
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        result = await service.handle_manager_attachments(
            message="Use the screenshot as visual context.",
            attachments=(
                ("spec.md", b"# Signal\n\nNo backtest.\n", "text/markdown"),
                ("notes.txt", b"BTC closed candles only", "text/plain"),
                ("reference.png", b"\x89PNG\r\n\x1a\nfixture", "image/png"),
            ),
            artifact_kind="signal",
            backtest_required=False,
        )

        assert result["action"] == "add_signal"
        assert result["uploaded_spec"]["backtest_required"] is False
        assert {item["name"] for item in result["uploaded_attachments"]} == {
            "notes.txt",
            "reference.png",
        }
        for item in result["uploaded_attachments"]:
            assert (tmp_path / item["path"]).is_file()
        order = result["snapshot"]["work_orders"][0]
        assert order["backtest_required"] is False
        assert order["dag"]["nodes"][-1]["node_id"] == "live_approval"

    asyncio.run(scenario())


def test_manager_requires_description_for_non_spec_attachment(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        try:
            await service.handle_manager_attachments(
                message="",
                attachments=(("reference.png", b"image", "image/png"),),
            )
        except ValueError as exc:
            assert "describe what Manager should do" in str(exc)
        else:
            raise AssertionError("an image without a user description must be rejected")

    asyncio.run(scenario())


def test_snapshot_exposes_inactive_implemented_registry_entry(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(False, None, None, "x.log", "runtime stopped")

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        manifest = tmp_path / "signals" / "implemented" / "sample-signal"
        manifest.mkdir(parents=True)
        (manifest / "1.1.0.toml").write_text('stable_id = "sample-signal"', encoding="utf-8")
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        snapshot = await service.snapshot()
        registry = {item["id"]: item for item in snapshot["live_registry"]}
        assert registry["sample-signal"]["running"] is False
        assert registry["sample-signal"]["status"] == "IMPLEMENTED"
        assert registry["sample-signal"]["version"] == "1.1.0"

    asyncio.run(scenario())


def test_snapshot_highlights_the_selected_runtime_signal(tmp_path: Path) -> None:
    class FakeRuntime:
        def status(self) -> Any:
            from quant_signal_agent.studio.gateway import RuntimeStatus

            return RuntimeStatus(
                True,
                321,
                None,
                "x.log",
                "running sample-signal",
                signal_id="sample-signal",
                signal_version="1.1.0",
            )

        async def close(self) -> None:
            return None

    async def scenario() -> None:
        manifest = tmp_path / "signals" / "implemented" / "sample-signal"
        manifest.mkdir(parents=True)
        (manifest / "1.1.0.toml").write_text(
            'stable_id = "sample-signal"', encoding="utf-8"
        )
        service = StudioService(
            project_root=tmp_path,
            store=JsonStateStore(tmp_path / "state.json"),
            runtime=FakeRuntime(),  # type: ignore[arg-type]
        )
        snapshot = await service.snapshot()
        registry = {item["id"]: item for item in snapshot["live_registry"]}
        assert registry["sample-signal"]["running"] is True
        assert registry["sample-signal"]["status"] == "RUNNING"

    asyncio.run(scenario())
