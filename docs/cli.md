# CLI Reference

## provero init

Create a new `provero.yaml` template.

```bash
provero init [PATH] [--from-source TYPE:TABLE]
```

| Argument / Flag | Default | Description |
|-----------------|---------|-------------|
| `PATH` | `provero.yaml` | Path for the config file |
| `--from-source` | | Generate checks by profiling a table. Format: `TYPE:TABLE` (e.g., `duckdb:orders`) |

**Examples:**

```bash
provero init
provero init my_checks.yaml
provero init --from-source duckdb:orders
provero init --from-source postgres:users
```

## provero run

Execute quality checks defined in the config file.

```bash
provero run [OPTIONS]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `provero.yaml` | Config file path |
| `--suite` | `-s` | | Run a specific suite only |
| `--tag` | `-t` | | Run suites matching this tag |
| `--format` | `-f` | `table` | Output format: `table`, `json`, `csv` |
| `--no-store` | | `false` | Don't persist results to SQLite |
| `--no-optimize` | | `false` | Disable SQL batching |
| `--no-alerts` | | `false` | Don't send webhook alerts |
| `--report` | | | Generate a report: `html` |

**Examples:**

```bash
provero run
provero run -c production.yaml
provero run --suite orders_quality
provero run --tag critical
provero run --format json
provero run --report html
provero run --no-store --no-alerts
```

**Exit codes:**

| Code | Meaning |
|------|---------|
| 0 | All checks passed (or only info/warning failures) |
| 1 | At least one critical/blocker check failed |

## provero validate

Validate config syntax without running checks.

```bash
provero validate [OPTIONS]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `provero.yaml` | Config file path |
| `--schema-only` | | `false` | Only validate against JSON Schema (skip semantic checks) |

**Examples:**

```bash
provero validate
provero validate -c production.yaml
provero validate --schema-only
```

## provero profile

Profile a data source and optionally suggest checks.

```bash
provero profile [OPTIONS]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `provero.yaml` | Config file path |
| `--table` | `-t` | | Table to profile (overrides config) |
| `--suggest` | | `false` | Suggest checks based on the profile |
| `--sample` | | | Sample size for large tables |

**Examples:**

```bash
provero profile
provero profile --table orders
provero profile --suggest
provero profile --table orders --suggest --sample 10000
```

## provero history

Show historical check results from the result store.

```bash
provero history [OPTIONS]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--suite` | `-s` | | Filter by suite name |
| `--limit` | `-n` | `20` | Number of runs to show |
| `--run` | `-r` | | Show details for a specific run ID |

**Examples:**

```bash
provero history
provero history --suite orders_quality
provero history -n 50
provero history --run abc12345
```

## provero contract validate

Validate data contracts against live data sources.

```bash
provero contract validate [OPTIONS]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--config` | `-c` | `provero.yaml` | Config file path |

**Examples:**

```bash
provero contract validate
provero contract validate -c production.yaml
```

## provero contract diff

Show differences between two contract versions. Reports added/removed/changed columns, type changes, and whether changes are breaking.

```bash
provero contract diff OLD_CONFIG NEW_CONFIG
```

| Argument | Description |
|----------|-------------|
| `OLD_CONFIG` | Path to the old config file |
| `NEW_CONFIG` | Path to the new config file |

**Example:**

```bash
provero contract diff v1.yaml v2.yaml
```

## provero watch

Continuously run checks on a polling interval. Useful for monitoring and CI.

```bash
provero watch                        # every 5 minutes (default)
provero watch --interval 30s         # every 30 seconds
provero watch -c staging.yaml -i 1m  # custom config, every minute
provero watch --count 3 --format json  # run 3 times, output JSONL
```

| Option | Short | Default | Description |
|--------|-------|---------|-------------|
| `--interval` | `-i` | `5m` | Polling interval (30s, 5m, 1h, 1h30m) |
| `--count` | `-n` | | Run exactly N times then exit |
| `--config` | `-c` | `provero.yaml` | Config file path |
| `--format` | `-f` | `table` | Output format: `table`, `json`, `csv` |
| `--suite` | `-s` | | Run a specific suite only |
| `--no-store` | | `false` | Don't persist results |

Press `Ctrl+C` to stop. Exit code 1 if any run has failures.

## provero import soda

Convert a SodaCL configuration file to Provero format.

```bash
provero import soda checks.yaml                      # print to stdout
provero import soda checks.yaml -o provero.yaml      # write to file
provero import soda checks.yaml --source-type postgres  # set source type
```

Supported SodaCL check mappings:

| SodaCL | Provero |
|--------|---------|
| `missing_count(col) = 0` | `not_null: col` |
| `duplicate_count(col) = 0` | `unique: col` |
| `missing_percent(col) < N%` | `completeness: {column, min}` |
| `invalid_count(col) = 0` + `valid values` | `accepted_values` |
| `row_count > N` | `row_count: {min}` |
| `freshness(col) < Xh` | `freshness: {column, max_age}` |

Unsupported checks are included as comments in the output.

## provero export dbt

Generate dbt `schema.yml` test definitions from Provero checks.

```bash
provero export dbt                          # read provero.yaml, print to stdout
provero export dbt -c checks.yaml -o schema.yml  # custom input/output
```

Supported mappings:

| Provero | dbt test |
|---------|----------|
| `not_null` | `not_null` |
| `unique` | `unique` |
| `accepted_values` | `accepted_values` |
| `range` | `dbt_utils.expression_is_true` |

Checks without a dbt equivalent are included as YAML comments.

## provero version

Show the installed Provero version.

```bash
provero version
```
