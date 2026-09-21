# CHA!N Quant Studio — User Specification

> Fill in the fields below in plain language. The Studio will generate the
> internal normalized spec, DAG, hashes, and approval records for you.

## 1. Identity

- Spec schema: `chain-user-spec/v1`
- Artifact type: `signal` or `strategy`
- Name: `<human-readable name>`
- Stable ID: `<lowercase-kebab-case-id>`
- Version: `1.0.0`
- Horizon: `short`, `medium`, or `long`
- Backtest required: `yes` or `no`

## 2. Purpose

Describe what should be detected or traded in two or three sentences.

## 3. Market

- Venue: `Binance`
- Market: `Spot`, `USD-M`, or `both`
- Symbols or universe: `<for example BTCUSDT, or top-100 market-cap union top-100 volume>`
- Timeframes: `<for example 4h, or 1h/2h/4h/1d/1w>`
- Evaluation event: `<for example closed candle only>`
- UTC boundary: `yes`

## 4. Exact behavior

State formulas, lookbacks, comparisons, and AND/OR grouping. For a Signal,
describe the neutral notification condition. For a Trading Strategy, also state
direction, entry, exit, position sizing, and risk.

## 5. Lifecycle and safety

- Duplicate/reset rule: `<when the same condition may notify or enter again>`
- Missing/stale/gap behavior: `fail closed; do not evaluate until repaired`
- Required data: `<inputs that may block evaluation>`
- Context-only data: `<optional enrichment that must not block the result>`

## 6. Acceptance examples

1. Must trigger: `<one concrete example>`
2. Must not trigger: `<one boundary example>`
3. Must fail closed: `<one stale, missing, or gap example>`

## 7. Open questions

Write `None` if every rule is executable without interpretation.
