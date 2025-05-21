<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate;

use GuzzleHttp\Client as GuzzleClient;
use GuzzleHttp\Psr7\Request;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ConfigurationRow;
use Keboola\StorageApi\Workspaces;
use Psr\Log\LoggerInterface;

class MigrateWriter
{
    private const KEBOOLA_SNOWFLAKE_HOSTS = [
        'keboola.eu-central-1.snowflakecomputing.com',
        'keboola.snowflakecomputing.com',
        'keboola.west-europe.azure.snowflakecomputing.com',
        'rl74503-com_keboola_gcp_us_east4.snowflakecomputing.com',
        'rl74503-com_keboola_gcp_europe_west3.snowflakecomputing.com',
        'rl74503-com_keboola_gcp_europe_west3_2.snowflakecomputing.com',
    ];

    /** @var string $sourceComponentId */
    private $sourceComponentId;

    /** @var string $destinationComponentId */
    private $destinationComponentId;

    /** @var Components */
    private $sourceComponentsApi;

    /** @var Components */
    private $destComponentsApi;

    /** @var Workspaces */
    private $destWorkspacesApi;

    /** @var GuzzleClient  */
    private $encryptionClient;

    /** @var LoggerInterface  */
    private $logger;

    /** @var array */
    private $destinationToken;

    /** @var bool */
    private $dryRun;

    public function __construct(
        Components $sourceComponentsApi,
        Components $destComponentsApi,
        Workspaces $destWorkspacesApi,
        GuzzleClient $encryptionClient,
        LoggerInterface $logger,
        array $destinationToken,
        string $sourceComponentId = Config::AWS_COMPONENT_ID,
        string $destinationComponentId = Config::AWS_COMPONENT_ID,
        bool $dryRun = false
    ) {
        $this->sourceComponentsApi = $sourceComponentsApi;
        $this->destComponentsApi = $destComponentsApi;
        $this->destWorkspacesApi = $destWorkspacesApi;
        $this->sourceComponentId = $sourceComponentId;
        $this->encryptionClient = $encryptionClient;
        $this->logger = $logger;
        $this->destinationComponentId = $destinationComponentId;
        $this->dryRun = $dryRun;
        $this->destinationToken = $destinationToken;
    }

    public function migrate(string $configurationId): void
    {
        $configuration = $this->sourceComponentsApi->getConfiguration(
            $this->sourceComponentId,
            $configurationId
        );

        if (self::isKeboolaProvisionedWriter($configuration['configuration'])
            && $this->destinationToken['owner']['hasSnowflake'] === true) {
            if ($this->dryRun === false) {
                $workspace = $this->destWorkspacesApi->createWorkspace();
                $configuration = $this->extendConfigurationWithParamsFromWorkspace(
                    $configuration,
                    $workspace
                );
            } else {
                $this->logger->info('[dry-run] Create workspace for provisioned Snowflake writer');
            }
        }

        $newConfiguration = new Configuration();
        $newConfiguration
            ->setComponentId($this->destinationComponentId)
            ->setConfigurationId($configuration['id'])
            ->setDescription($configuration['description'])
            ->setName($configuration['name'])
            ->setRowsSortOrder($configuration['rowsSortOrder'])
            ->setConfiguration($configuration['configuration'])
            ->setState($configuration['state']);

        if ($this->dryRun === false) {
            $this->destComponentsApi->addConfiguration($newConfiguration);
        } else {
            $this->logger->info(sprintf(
                '[dry-run] Migrate configuration %s (component "%s")',
                $configuration['id'],
                $this->destinationComponentId
            ));
        }

        if (!empty($configuration['rows'])) {
            foreach ($configuration['rows'] as $row) {
                $newConfigurationRow = new ConfigurationRow($newConfiguration);
                $newConfigurationRow
                    ->setRowId($row['id'])
                    ->setName($row['name'])
                    ->setConfiguration($row['configuration'])
                    ->setChangeDescription($row['changeDescription'])
                    ->setDescription($row['description'])
                    ->setState($row['state'])
                    ->setIsDisabled($row['isDisabled']);

                if ($this->dryRun === false) {
                    $this->destComponentsApi->addConfigurationRow($newConfigurationRow);
                } else {
                    $this->logger->info(sprintf(
                        '[dry-run] Migrate row %s of configuration %s (component "%s")',
                        $row['id'],
                        $configuration['id'],
                        $this->destinationComponentId
                    ));
                }
            }
        }
    }

    private function extendConfigurationWithParamsFromWorkspace(array $sourceConfiguration, array $workspace): array
    {
        $request = new Request(
            'POST',
            sprintf('/encrypt?componentId=%s', $this->destinationComponentId),
            ['Content-Type' => 'text/plain'],
            $workspace['connection']['password']
        );

        $response = $this->encryptionClient->send($request);

        return array_replace_recursive(
            $sourceConfiguration,
            [
                'configuration' => [
                    'parameters' => [
                        'db' => [
                            'host' => $workspace['connection']['host'],
                            'user' => $workspace['connection']['user'],
                            '#password' => $response->getBody()->getContents(),
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
        return in_array(
            $configurationData['parameters']['db']['host'] ?? [],
            self::KEBOOLA_SNOWFLAKE_HOSTS,
            true
        );
    }
}
