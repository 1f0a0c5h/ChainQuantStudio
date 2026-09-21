# CHA!N Quant Studio Architecture

CHA!N is a desktop-first control plane for versioned quantitative research. The
public distribution is deliberately empty of concrete Signal Definitions and
Trading Strategies.

```mermaid
flowchart TB
  subgraph Clients[Client surfaces]
    D[Tauri desktop app\nlocal control + approvals]
    W[Public web\nredacted display only]
  end
  D --> G[Local Python Gateway]
  W -. redacted snapshot .-> G
  G --> M[Manager Agent\nintake + optional DAG orchestration]
  M --> S[Strategy Agent\nimplement versioned artifact]
  S --> R[Review Agent\nconsolidated independent audit]
  R --> I{Implementation approval}
  I -->|backtest requested| DS[Data Service\ncontent-addressed datasets]
  I -->|backtest omitted| L{Live approval}
  DS --> B[Backtest Agent\nreplay + metrics + charts]
  B --> R2[Review Agent\nevidence audit]
  R2 -->|directional strategy| O[Optimization Agent\nread-only proposal]
  O --> OD{Optimization decision}
  OD -->|approve revision| S
  OD -->|keep version| BA{Backtest approval}
  R2 -->|neutral signal| BA
  BA --> L
  L -->|approved signal| SG[Signal Agent\nadvisory runtime]
  L -. research only .-> T[Trading Agent\nLOCKED]
  SG -->|incident| X[Maintenance Agent]
  X -->|verified repair| SG
  AR[(Artifact Registry\nspec + code + dataset + tests + reports)]
  S --> AR
  DS --> AR
  B --> AR
  R --> AR
  R2 --> AR
  O --> AR
  CB[Global Codex circuit breaker] --> M
  Q[(Retry budget + checkpoints + dead letters)] --> M
```

## Contracts

- A work order is a selectable DAG, not a mandatory linear pipeline.
- `Backtest required: no` omits Data Service and Backtest nodes; no stage is
  falsely marked complete.
- Implementation, backtest, optimization, and live decisions are distinct gates.
- Review is read-only and reports all blocking findings in one consolidated pass.
- Optimization is read-only, cannot mine unrestricted parameters, cannot change
  code, and cannot approve its own proposal.
- Data Service is deterministic software, not an LLM agent. Dataset identity is
  derived from venue, market, symbol, timeframe, range, schema, and content hash.
- Artifact Registry links each work order to immutable provenance and evidence.
- Retry budgets, checkpoints, dead letters, and one global provider circuit
  breaker prevent infinite retry loops.
- The Signal Agent can run only an exact live-approved artifact version.
- The Trading Agent remains locked and has no authenticated exchange capability.

## Deployment

- **Desktop (primary):** Tauri hosts the React control surface and connects to the
  local Python Gateway for process, file, approval, and report operations.
- **Public web:** redacted display/demo only. It cannot issue commands, upload
  files, approve work, control processes, read local paths, or expose raw logs.

The public repository starts with empty artifact registries. Concrete signals,
strategies, datasets, reports, and runtime state are local/private inputs.
