# app-snowflake-writer-migrate – AI Development Context

## What this repository does

Keboola App component for migrating Snowflake Writer configurations. Run by `app-project-migrate` as Phase 6 (only if `migrateSecrets: false`). Component ID: `keboola.app-snowflake-writer-migrate`.

## Documentation

- **`docs/overview.md`** – writer migration logic, processed component IDs, dynamic selection by stack
- **`docs/configuration.md`** – complete input parameter reference

## Required environment variables

Before running tests, verify that these variables are present in `.env` (or exported in the shell). If missing, ask for them explicitly.

**Platform-injected (automatically set by Keboola Runner when running in Keboola):**

| Variable | Description |
|---|---|
| `KBC_URL` | URL of the destination Keboola project |
| `KBC_TOKEN` | Storage token of the destination project |
| `KBC_RUNID` | Job run ID, set on the SAPI client |

> This component does not have its own integration tests with live credentials – it is tested via datadir functional tests or directly in the Keboola platform. Check that the `.env` file exists in the repo root.

## Development commands

Service name in `docker-compose.yml` is `dev`.

```bash
docker compose run --rm dev composer phpcs
docker compose run --rm dev composer phpstan
docker compose run --rm dev composer tests           # tests-phpunit
docker compose run --rm dev composer build           # phplint + phpcs + phpstan + tests
```

## Key files

| File | Purpose |
|---|---|
| `src/Component.php` | Entry point + dynamic target component ID selection by `defaultBackend` |
| `src/MigrateWriter.php` | Core writer migration logic |
| `src/Config.php` | Configuration getters + `getSourceComponentId()` – dynamic selection by source URL |
| `src/ConfigDefinition.php` | Input parameter validation |

## Processed components

```
keboola.wr-db-snowflake           (AWS)
keboola.wr-snowflake-blob-storage  (Azure)
keboola.wr-db-snowflake-gcs        (GCP)
keboola.wr-db-snowflake-gcs-s3     (GCP with S3)
```

## Logic

**Keboola-provisioned writer:** Creates a new workspace in the destination project → fills credentials → encrypts password → creates configuration + rows.

**BYO writer:** Configuration is copied unchanged. Credentials must be manually updated.

## Coding standards

- PHP **7.1+** (`composer.json` requires `"php": "^7.1"`) – unlike other repos, this is NOT PHP 8
- Strict types (`declare(strict_types=1)`)
- PHPStan level max
- Keboola coding standard (PSR-12)

## Related repositories

- Orchestrator: `app-project-migrate`
