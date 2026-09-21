from __future__ import annotations

import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_repo_skills_have_matching_frontmatter_and_no_placeholders() -> None:
    skill_files = sorted((ROOT / ".agents" / "skills").glob("*/SKILL.md"))

    assert {path.parent.name for path in skill_files} == {
        "artifact-release-review",
        "backtest-provenance-audit",
        "code-change-verification",
        "final-release-review",
        "live-registry-verification",
        "market-replay-verification",
        "quant-change-intake",
        "strategy-integrity-review",
        "workflow-state-recovery",
        "exchange-data-integrity-review",
    }
    for path in skill_files:
        text = path.read_text(encoding="utf-8")
        assert text.startswith("---\n")
        frontmatter = text.split("---", 2)[1]
        fields = {
            key.strip(): value.strip()
            for line in frontmatter.splitlines()
            if ":" in line
            for key, value in (line.split(":", 1),)
        }
        assert fields["name"] == path.parent.name
        assert fields["description"]
        assert "TODO" not in text


def test_project_agents_cover_reviewers_and_quant_studio_roles() -> None:
    agent_files = sorted((ROOT / ".codex" / "agents").glob("*.toml"))
    definitions = {
        data["name"]: data
        for path in agent_files
        for data in (tomllib.loads(path.read_text(encoding="utf-8")),)
    }

    assert len(agent_files) == 10
    assert {
        "manager_agent",
        "strategy_agent",
        "backtest_agent",
        "review_agent",
        "signal_agent",
        "maintenance_agent",
        "trading_agent",
    }.issubset(definitions)
    assert len(definitions) == len(agent_files)
    for data in definitions.values():
        assert data["description"]
        assert data["developer_instructions"]
    for name in (
        "strategy_integrity_reviewer",
        "test_reliability_reviewer",
        "exchange_reviewer",
        "manager_agent",
        "trading_agent",
    ):
        assert definitions[name]["sandbox_mode"] == "read-only"
    for name in (
        "strategy_agent",
        "backtest_agent",
        "review_agent",
        "signal_agent",
        "maintenance_agent",
    ):
        assert definitions[name]["sandbox_mode"] == "workspace-write"
    trading_instructions = definitions["trading_agent"]["developer_instructions"]
    assert "hard standby" in trading_instructions
    assert "Do not connect authenticated endpoints" in trading_instructions
    manager_instructions = definitions["manager_agent"]["developer_instructions"]
    assert "strict\nallowlist" in manager_instructions
    assert "Do not answer requests outside" in manager_instructions
    assert "Never edit, patch, create, delete, refactor, or configure" in manager_instructions


def test_nested_instruction_files_cover_scheduled_boundaries() -> None:
    expected = (
        ROOT / "src" / "quant_signal_agent" / "exchanges" / "AGENTS.md",
        ROOT / "src" / "quant_signal_agent" / "strategies" / "AGENTS.md",
        ROOT / "src" / "quant_signal_agent" / "backtesting" / "AGENTS.md",
        ROOT / "tests" / "AGENTS.md",
        ROOT / "tools" / "AGENTS.md",
    )

    assert all(
        path.is_file() and path.read_text(encoding="utf-8").strip()
        for path in expected
    )
