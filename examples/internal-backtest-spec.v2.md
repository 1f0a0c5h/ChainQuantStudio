# Synthetic internal backtest contract

This developer example demonstrates the safe internal schema only. It is not a
user upload template, bundled Signal Definition, or bundled Trading Strategy and
is never registered for live use.

```toml qsa-backtest
schema_version = 2
engine = "vectorized-feature-dsl-v1"
mode = "signal_research"

[strategy]
id = "framework-contract-example"
version = "0-test"
horizon = "short_term"
direction = "neutral"

[data]
provider = "normalized_csv"
catalog_path = "data/backtest/example"
start = 2020-01-01T00:00:00Z
end = 2020-02-01T00:00:00Z
timezone = "UTC"
timeframes = ["1h"]
source_columns = [
  "spot_quote_volume",
  "perpetual_quote_volume",
  "perpetual_high",
  "perpetual_low",
  "perpetual_close",
]
trigger_inputs = ["spot_ohlcv", "perpetual_ohlcv"]
supplemental_inputs = ["open_interest"]
closed_candles_only = true
public_only = true

[universe]
kind = "fixed"
symbols = ["SAMPLE/USDT"]
ranking_days = 1
top_volume = 1
top_gainers = 1
exclude_bases = []
classification = "none"

[[parameters]]
id = "lookback_hours"
values = [2, 4]

[[parameters]]
id = "volume_ratio"
values = [2.0, 3.0]

[[features]]
id = "spot_volume_ratio"
operator = "rolling_ratio"
inputs = ["spot_quote_volume"]
window_parameter = "lookback_hours"

[[features]]
id = "perpetual_volume_ratio"
operator = "rolling_ratio"
inputs = ["perpetual_quote_volume"]
window_parameter = "lookback_hours"

[[features]]
id = "confirmed_volume_ratio"
operator = "minimum"
inputs = ["spot_volume_ratio", "perpetual_volume_ratio"]

[[features]]
id = "example_condition"
operator = "greater_equal"
inputs = ["confirmed_volume_ratio"]
threshold_parameter = "volume_ratio"

[signal]
condition = "example_condition"
event_policy = "onset"
cooldown_seconds = 0

[label]
operator = "future_absolute_move"
high_column = "perpetual_high"
low_column = "perpetual_low"
close_column = "perpetual_close"
horizon_hours = 4
threshold = 0.01

[validation]
method = "chronological_holdout"
selection_fraction = 0.8
holdout_fraction = 0.2
primary_metric = "wilson_lower"
max_signals_per_symbol_day = 24.0

[safety]
signal_only = true
allow_execution = false
allow_authenticated_exchange_api = false
auto_promote_parameters = false
```
