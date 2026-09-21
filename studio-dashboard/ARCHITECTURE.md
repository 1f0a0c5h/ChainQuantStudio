# CHA!N Quant Studio UI Architecture

```text
User
  -> Manager Agent (semantic intake + hard scope gate)
      -> optional versioned work DAG
          -> Strategy Agent (Signal Definition or Trading Strategy)
          -> Review Agent (one consolidated independent audit)
          -> implementation approval
          -> optional Data Service + Backtest Agent
          -> Review Agent (backtest evidence)
          -> optional Optimization Agent + user decision
          -> backtest approval
          -> live approval
              -> Signal Agent (advisory runtime)
              -> Trading Agent (LOCKED)
          -> runtime incident -> Maintenance Agent -> verified recovery

Public market data -> normalized/fresh state -> approved artifact runtime
Artifact Registry <- spec/code/dataset/test/report provenance from every stage
```

The public build contains no concrete signal or strategy artifacts. Desktop is the
authoritative local control surface; a web build is display-only. The UI and Gateway
cannot place, amend, or cancel exchange orders.
