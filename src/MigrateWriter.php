<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate;

use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Workspaces;

class MigrateWriter
{
    private const KEBOOLA_SNOWFLAKE_HOSTS = [
        'keboola.eu-central-1.snowflakecomputing.com',
        'keboola.snowflakecomputing.com',
    ];

    public const DEFAULT_COMPONENT_ID = 'keboola.wr-db-snowflake';

    /** @var string $componentId */
    private $componentId;

    /** @var Components */
    private $sourceComponentsApi;

    /** @var Components */
    private $destComponentsApi;

    /** @var Workspaces */
    private $destWorkspacesApi;

    public function __construct(
        Components $sourceComponentsApi,
        Components $destComponentsApi,
        Workspaces $destWorkspacesApi,
        string $componentId = self::DEFAULT_COMPONENT_ID
    ) {
        $this->sourceComponentsApi = $sourceComponentsApi;
        $this->destComponentsApi = $destComponentsApi;
        $this->destWorkspacesApi = $destWorkspacesApi;
        $this->componentId = $componentId;
    }

    public function migrate(string $configurationId): void
    {
        $configuration = $this->sourceComponentsApi->getConfiguration(
            $this->componentId,
            $configurationId
        );

        if (self::isKeboolaProvisionedWriter($configuration['configuration'])) {
            $workspace = $this->destWorkspacesApi->createWorkspace();
            $configuration = $this->extendConfigurationWithParamsFromWorkspace(
                $configuration,
                $workspace
            );
        }

        $newConfiguration = new Configuration();
        $newConfiguration
            ->setComponentId($this->componentId)
            ->setConfigurationId($configuration['id'])
            ->setDescription($configuration['description'])
            ->setName($configuration['name'])
            ->setRowsSortOrder($configuration['rowsSortOrder'])
            ->setConfiguration($configuration['configuration'])
            ->setState($configuration['state']);

        $this->destComponentsApi->addConfiguration($newConfiguration);
    }

    private function extendConfigurationWithParamsFromWorkspace(array $sourceConfiguration, array $workspace): array
    {
        return array_replace_recursive(
            $sourceConfiguration,
            [
                'configuration' => [
                    'parameters' => [
                        'db' => [
                            'host' => $workspace['connection']['host'],
                            'user' => $workspace['connection']['user'],
                            'password' => $workspace['connection']['password'],
                            'database' => $workspace['connection']['database'],
                            'schema' => $workspace['connection']['schema'],
                            'warehouse' => $workspace['connection']['warehouse'],
                        ],
                    ],
                ],
            ]
        );
    }

    public static function isKeboolaProvisionedWriter(array $configurationData): bool
    {
        if (!isset($configurationData['parameters']['db']['host'])) {
            return false;
        }

        return in_array(
            $configurationData['parameters']['db']['host'],
            self::KEBOOLA_SNOWFLAKE_HOSTS
        );
    }
}
