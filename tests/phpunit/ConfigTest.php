<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate\Tests;

use Keboola\AppSnowflakeWriterMigrate\Config;
use Keboola\AppSnowflakeWriterMigrate\ConfigDefinition;
use PHPUnit\Framework\TestCase;

class ConfigTest extends TestCase
{
    /**
     * @dataProvider sourceComponentIdProvider
     * @param string[] $expected
     */
    public function testGetSourceComponentId(string $sourceProjectUrl, array $expected): void
    {
        $config = $this->buildConfig($sourceProjectUrl);
        $this->assertSame($expected, $config->getSourceComponentId());
    }

    public function sourceComponentIdProvider(): iterable
    {
        yield 'US East 1 (AWS)' => [
            'https://connection.keboola.com',
            [Config::AWS_COMPONENT_ID],
        ];
        yield 'North Europe (Azure)' => [
            'https://connection.north-europe.azure.keboola.com',
            [Config::AZURE_COMPONENT_ID],
        ];
        yield 'HCI (Azure single-tenant)' => [
            Config::HCI_SOURCE_URL,
            [Config::AZURE_COMPONENT_ID],
        ];
        yield 'HCI with trailing slash' => [
            Config::HCI_SOURCE_URL . '/',
            [Config::AZURE_COMPONENT_ID],
        ];
        yield 'Europe West 3 (GCP)' => [
            'https://connection.europe-west3.gcp.keboola.com',
            [Config::GCP_COMPONENT_ID, Config::GCP_S3_COMPONENT_ID],
        ];
        yield 'US East 4 (GCP)' => [
            'https://connection.us-east4.gcp.keboola.com',
            [Config::GCP_COMPONENT_ID, Config::GCP_S3_COMPONENT_ID],
        ];
        yield 'Unknown stack falls back to AWS' => [
            'https://connection.example.keboola.cloud',
            [Config::AWS_COMPONENT_ID],
        ];
    }

    public function testHardcodedTargetComponentIdForHci(): void
    {
        $config = $this->buildConfig(Config::HCI_SOURCE_URL);
        $this->assertSame(Config::GCP_S3_COMPONENT_ID, $config->getHardcodedTargetComponentId());
    }

    public function testHardcodedTargetComponentIdForHciWithTrailingSlash(): void
    {
        $config = $this->buildConfig(Config::HCI_SOURCE_URL . '/');
        $this->assertSame(Config::GCP_S3_COMPONENT_ID, $config->getHardcodedTargetComponentId());
    }

    /**
     * @dataProvider nonHciSourceUrlProvider
     */
    public function testHardcodedTargetComponentIdReturnsNullForNonHciStacks(string $url): void
    {
        $config = $this->buildConfig($url);
        $this->assertNull($config->getHardcodedTargetComponentId());
    }

    public function nonHciSourceUrlProvider(): iterable
    {
        yield 'US East 1 (AWS)' => ['https://connection.keboola.com'];
        yield 'North Europe (Azure)' => ['https://connection.north-europe.azure.keboola.com'];
        yield 'Europe West 3 (GCP)' => ['https://connection.europe-west3.gcp.keboola.com'];
        yield 'US East 4 (GCP)' => ['https://connection.us-east4.gcp.keboola.com'];
        yield 'Unknown stack' => ['https://connection.example.keboola.cloud'];
    }

    private function buildConfig(string $sourceProjectUrl): Config
    {
        return new Config(
            [
                'parameters' => [
                    'sourceKbcUrl' => $sourceProjectUrl,
                    '#sourceKbcToken' => 'some-token',
                ],
            ],
            new ConfigDefinition()
        );
    }
}
