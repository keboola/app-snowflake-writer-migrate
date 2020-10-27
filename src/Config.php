<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const AWS_COMPONENT_ID = 'keboola.wr-db-snowflake';

    public const AZURE_COMPONENT_ID = 'keboola.wr-snowflake-blob-storage';

    public function getSourceProjectUrl(): string
    {
        return $this->getValue(['parameters', 'sourceKbcUrl']);
    }

    public function getSourceProjectToken(): string
    {
        return $this->getValue(['parameters', '#sourceKbcToken']);
    }

    public function getSourceComponentId(): string
    {
        switch ((string) getenv('KBC_URL')) {
            case 'connection.north-europe.azure.keboola.com':
                return self::AZURE_COMPONENT_ID;
            default:
                return self::AWS_COMPONENT_ID;
        }
    }
}
