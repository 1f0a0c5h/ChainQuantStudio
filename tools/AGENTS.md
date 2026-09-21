# Research and verification tool rules

- Keep holdout data isolated from parameter search and report sample dates explicitly.
- Label proxy inputs and distinguish research output from runtime thresholds.
- Make results reproducible from documented inputs, arguments, and code versions.
- Never write secrets, authenticated exchange access, or order execution into tools.
- Secret scanners must report file paths and pattern categories, never secret values.
- Tool changes require deterministic tests where practical.
- Prefer the canonical `qsa-spec`, `qsa-data`, `qsa-artifacts`, and `qsa-release`
  interfaces over adding another one-off workflow script.
- Data Service inputs and Artifact Registry entries must be content-addressed and
  verified before reuse. A status/verification tool must not silently repair or
  approve state.
- Keep historical one-off research scripts until their published evidence can be
  reproduced through the canonical engine; then retain only a thin compatibility
  wrapper or archive them deliberately.
