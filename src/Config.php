<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const AWS_COMPONENT_ID = 'keboola.wr-db-snowflake';

    public const AZURE_COMPONENT_ID = 'keboola.wr-snowflake-blob-storage';

    public const GCP_COMPONENT_ID = 'keboola.wr-db-snowflake-gcs';

    public const GCP_S3_COMPONENT_ID = 'keboola.wr-db-snowflake-gcs-s3';


    public function getSourceProjectUrl(): string
    {
        return $this->getValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceProjectToken(): string
    {
        return $this->getValue(['parameters', '#sourceKbcToken']);
    }

    /**
     * Returns all known Snowflake writer component IDs as source candidates.
     *
     * Each source stack only has a single Snowflake writer component installed (matching its
     * file-storage backend), so listing configurations for a component that isn't installed on
     * the source project simply returns an empty array. Iterating the full set makes the migration
     * stack-agnostic and removes the need to maintain a hard-coded source URL switch.
     *
     * @return string[]
     */
    public function getSourceComponentId(): array
    {
        return [
            self::AWS_COMPONENT_ID,
            self::AZURE_COMPONENT_ID,
            self::GCP_COMPONENT_ID,
            self::GCP_S3_COMPONENT_ID,
        ];
    }

    public function isDryRun(): bool
    {
        return $this->getValue(['parameters', 'dryRun']);
    }
}
