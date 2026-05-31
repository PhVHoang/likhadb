## Agent skills

### Issue tracker

Issues are tracked in GitHub Issues (`github.com/PhVHoang/likhadb`). See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the five default canonical label strings. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context repo — one `CONTEXT.md` + `docs/adr/` at the root. See `docs/agents/domain.md`.

### Bash commands

Prefer these over defaults when available. Fall back silently if missing.

- **Search content:** `rg` over `grep`
- **Find files:** `fd` over `find`
- **Never** use `find -exec` or `xargs` chains when `fd -x` or `rg -l | xargs` would be clearer. Prefer readable pipelines.
- **Structural/AST search:** `ast-grep` (`sg`) for refactors and pattern-based code search, especially in TS/TSX
- **JSON:** `jq` for any parsing, filtering, or transformation in pipelines
- **YAML/TOML:** `yq`
- **GitHub operations:** `gh` for PRs, issues, reviews, CI status, and releases. Do not scrape github.com or hit the REST API directly when `gh` can do it.
- **Benchmarking:** `hyperfine` when comparing command performance

Prefer these over defaults when available. Fall back silently if missing.

- **Search content:** `rg` over `grep`
- **Find files:** `fd` over `find`
- **Never** use `find -exec` or `xargs` chains when `fd -x` or `rg -l | xargs` would be clearer. Prefer readable pipelines.
- **Structural/AST search:** `ast-grep` (`sg`) for refactors and pattern-based code search, especially in TS/TSX
- **JSON:** `jq` for any parsing, filtering, or transformation in pipelines
- **YAML/TOML:** `yq`
- **GitHub operations:** `gh` for PRs, issues, reviews, CI status, and releases. Do not scrape github.com or hit the REST API directly when `gh` can do it.
- **Benchmarking:** `hyperfine` when comparing command performance
