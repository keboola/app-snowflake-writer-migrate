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

    public const HCI_SOURCE_URL = 'https://connection.hci.keboola.cloud';

    public const EUW3_GCP_DESTINATION_URL = 'https://connection.europe-west3.gcp.keboola.com';


    public function getSourceProjectUrl(): string
    {
        return $this->getValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceProjectToken(): string
    {
        return $this->getValue(['parameters', '#sourceKbcToken']);
    }

    /**
     * @return string[]
     */
    public function getSourceComponentId(): array
    {
        switch (rtrim($this->getSourceProjectUrl(), '/')) {
            case self::HCI_SOURCE_URL:
            case 'https://connection.north-europe.azure.keboola.com':
                return [self::AZURE_COMPONENT_ID];
            case 'https://connection.europe-west3.gcp.keboola.com':
            case 'https://connection.us-east4.gcp.keboola.com':
                return [self::GCP_COMPONENT_ID, self::GCP_S3_COMPONENT_ID];
            default:
                return [self::AWS_COMPONENT_ID];
        }
    }

    /**
     * Hard-coded target component for a specific migration path. Returns null when no override
     * applies and the caller should fall back to `image_parameters.componentId`.
     */
    public function getHardcodedTargetComponentId(): ?string
    {
        $source = rtrim($this->getSourceProjectUrl(), '/');
        if ($source === self::HCI_SOURCE_URL) {
            return self::GCP_S3_COMPONENT_ID;
        }
        return null;
    }

    public function isDryRun(): bool
    {
        return $this->getValue(['parameters', 'dryRun']);
    }
}
