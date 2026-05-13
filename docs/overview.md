# app-snowflake-writer-migrate – documentation

## What the component does

Migrates Snowflake Writer configurations from a source project to a destination. Snowflake Writers are a special case because they have workspace credentials that cannot be simply transferred – they must be recreated in the destination project.

Run by `app-project-migrate` as Phase 6 (only if `migrateSecrets: false`). If secrets are being migrated, Snowflake Writers are handled directly by the Encryption API in Phase 4.

## Processed components

| Component ID | Stack |
|---|---|
| `keboola.wr-db-snowflake` | AWS |
| `keboola.wr-snowflake-blob-storage` | Azure |
| `keboola.wr-db-snowflake-gcs` | GCP |
| `keboola.wr-db-snowflake-gcs-s3` | GCP with S3 |

### Dynamic selection by source stack (`Config::getSourceComponentId`)

The component does not iterate through all 4 Writer component IDs – the specific set is selected based on the source project URL:

| Source stack URL | Searched source component IDs |
|---|---|
| `https://connection.north-europe.azure.keboola.com` | `keboola.wr-snowflake-blob-storage` |
| `https://connection.europe-west3.gcp.keboola.com`, `https://connection.us-east4.gcp.keboola.com` | `keboola.wr-db-snowflake-gcs`, `keboola.wr-db-snowflake-gcs-s3` |
| other (default) | `keboola.wr-db-snowflake` |

### Dynamic target component ID selection (`Component.php`)

The target component ID comes from `imageParameters.componentId`. If it is `keboola.wr-db-snowflake-gcs` or `keboola.wr-db-snowflake-gcs-s3`, the destination project's `defaultBackend` further determines:
- `defaultBackend === 'snowflake'` → `keboola.wr-db-snowflake-gcs-s3`
- otherwise → `keboola.wr-db-snowflake-gcs`

## Migration logic (MigrateWriter.php)

### For Keboola-provisioned writers

If the writer is provisioned (Keboola manages the Snowflake workspace) and the destination project has the Snowflake feature:

1. Loads configuration from source project
2. Creates a new workspace in the destination project
3. Fills in configuration parameters from the workspace (host, user, password, database, schema, warehouse)
4. Encrypts password via Encryption API
5. Creates new configuration in destination project
6. Adds all configuration rows

### For BYO (non-provisioned) writers

Configuration is copied unchanged (without workspace manipulation). Credentials remain from source – users must manually update them.

## Configuration parameters

| Parameter | Default | Description |
|---|---|---|
| `sourceKbcUrl` | `https://connection.keboola.com` | URL of the source project |
| `#sourceKbcToken` | – | Storage token of the source project |
| `dryRun` | `false` | Simulation without actual changes (passed by `app-project-migrate` orchestrator) |

## Key files

| File | Description |
|---|---|
| `src/Component.php` | Entry point |
| `src/MigrateWriter.php` | Core writer migration logic |
| `src/Config.php` | Configuration getters |

## Development and testing

```bash
docker compose run --rm dev composer phpcs
docker compose run --rm dev composer phpstan
docker compose run --rm dev composer tests
```

## Related repositories

- Used by: `app-project-migrate` (Phase 6)
- Component ID: `keboola.app-snowflake-writer-migrate`
