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

    public const SNOWFLAKE_WRITER_COMPONENT_IDS = [
        self::AWS_COMPONENT_ID,
        self::AZURE_COMPONENT_ID,
        self::GCP_COMPONENT_ID,
        self::GCP_S3_COMPONENT_ID,
    ];

    public function getSourceProjectUrl(): string
    {
        return $this->getValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceProjectToken(): string
    {
        return $this->getValue(['parameters', '#sourceKbcToken']);
    }

    /**
     * @param string[] $availableSourceComponentIds Component ids installed on the source stack.
     * @return string[]
     */
    public function getSourceComponentIds(array $availableSourceComponentIds): array
    {
        return array_values(array_intersect(
            self::SNOWFLAKE_WRITER_COMPONENT_IDS,
            $availableSourceComponentIds
        ));
    }

    public function isDryRun(): bool
    {
        return $this->getValue(['parameters', 'dryRun']);
    }
}
