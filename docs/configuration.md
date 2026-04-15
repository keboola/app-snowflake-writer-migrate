# Configuration parameter reference – app-snowflake-writer-migrate

## Parameters

| Parameter | Default | Description |
|---|---|---|
| `sourceKbcUrl` | `https://connection.keboola.com` | URL of the source Keboola project |
| `#sourceKbcToken` | – (required) | Storage token of the source project |
| `dryRun` | `false` | Simulation without actual changes (passed by `app-project-migrate` orchestrator) |

## Processed components

Specific Writer component IDs are selected **dynamically based on the source project URL** (`Config::getSourceComponentId`):

| Source stack URL | Searched source component IDs |
|---|---|
| `connection.north-europe.azure.keboola.com` | `keboola.wr-snowflake-blob-storage` |
| `connection.europe-west3.gcp.keboola.com`, `connection.us-east4.gcp.keboola.com` | `keboola.wr-db-snowflake-gcs`, `keboola.wr-db-snowflake-gcs-s3` |
| other (default) | `keboola.wr-db-snowflake` |

Target component ID comes from `imageParameters.componentId`. For GCP writers, the destination project's `defaultBackend` further determines (`snowflake` → `keboola.wr-db-snowflake-gcs-s3`, otherwise `keboola.wr-db-snowflake-gcs`).

## Migration behavior

### Keboola-provisioned writer
If the destination project has the Snowflake feature, a new workspace is created and the configuration is filled with new credentials. Password is encrypted via Encryption API.

### BYO (non-provisioned) writer
Configuration is copied unchanged. Source credentials remain – **users must manually update them after migration**.

## Configuration example

```json
{
  "parameters": {
    "sourceKbcUrl": "https://connection.keboola.com",
    "#sourceKbcToken": "xxx"
  }
}
```

## Notes

- This component is only run if `migrateSecrets: false` in `app-project-migrate`. If secrets are being migrated, Snowflake Writers are handled directly by the Encryption API as part of Phase 4.
- Configuration rows are always migrated together with the parent configuration.
