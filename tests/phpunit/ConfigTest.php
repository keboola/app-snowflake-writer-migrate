<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate\Tests;

use Keboola\AppSnowflakeWriterMigrate\Config;
use Keboola\AppSnowflakeWriterMigrate\ConfigDefinition;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider sourceProjectUrlProvider
     */
    public function testGetSourceComponentIdReturnsAllKnownWriters(string $sourceProjectUrl): void
    {
        $config = new Config(
            [
                'parameters' => [
                    'sourceKbcUrl' => $sourceProjectUrl,
                    '#sourceKbcToken' => 'some-token',
                ],
            ],
            new ConfigDefinition()
        );

        $this->assertSame(
            [
                Config::AWS_COMPONENT_ID,
                Config::AZURE_COMPONENT_ID,
                Config::GCP_COMPONENT_ID,
                Config::GCP_S3_COMPONENT_ID,
            ],
            $config->getSourceComponentId()
        );
    }

    public function sourceProjectUrlProvider(): iterable
    {
        yield 'US East 1 (AWS)' => ['https://connection.keboola.com'];
        yield 'EU Central 1 (AWS)' => ['https://connection.eu-central-1.keboola.com'];
        yield 'North Europe (Azure)' => ['https://connection.north-europe.azure.keboola.com'];
        yield 'HCI (Azure single-tenant)' => ['https://connection.hci.keboola.cloud'];
        yield 'Europe West 3 (GCP)' => ['https://connection.europe-west3.gcp.keboola.com'];
        yield 'US East 4 (GCP)' => ['https://connection.us-east4.gcp.keboola.com'];
        yield 'Custom / unknown stack' => ['https://connection.example.keboola.cloud'];
        yield 'URL with trailing slash' => ['https://connection.hci.keboola.cloud/'];
    }
}
