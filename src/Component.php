<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate;

use Keboola\Component\BaseComponent;
use Keboola\StorageApi\ClientException as StorageClientException;
use Keboola\StorageApi\Client as StorageClient;
use Keboola\Component\UserException;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\Workspaces;

class Component extends BaseComponent
{
    public function run(): void
    {
        /** @var Config $config */
        $config = $this->getConfig();
        $logger = $this->getLogger();

        $sourceProjectClient = $this->createStorageClient([
            'url' => $config->getSourceProjectUrl(),
            'token' => $config->getSourceProjectToken(),
        ]);
        try {
            $sourceTokenInfo = $sourceProjectClient->verifyToken();
        } catch (StorageClientException $e) {
            throw new UserException('Cannot authorize source project: ' . $e->getMessage(), $e->getCode(), $e);
        }

        $destProjectClient = $this->createStorageClient([
            'url' => getenv('KBC_URL'),
            'token' => getenv('KBC_TOKEN'),
        ]);

        $logger->info(sprintf(
            'Migrating Snowflake writers from project %s (%d) at %s',
            $sourceTokenInfo['owner']['name'],
            $sourceTokenInfo['owner']['id'],
            $config->getSourceProjectUrl()
        ));

        $sourceProjectComponentsApi = new Components($sourceProjectClient);

        $componentId = $config->getImageParameters()['componentId'];

        $migrate = new MigrateWriter(
            $sourceProjectComponentsApi,
            new Components($destProjectClient),
            new Workspaces($destProjectClient),
            $componentId
        );
        $writers = $sourceProjectComponentsApi->listComponentConfigurations(
            (new ListComponentConfigurationsOptions())
            ->setComponentId($componentId)
        );
        foreach ($writers as $writer) {
            $logger->info(sprintf(
                'Migration of writer %s (%s)',
                $writer['name'],
                $writer['id']
            ));
            $migrate->migrate($writer['id']);
        }
    }

    private function createStorageClient(array $params): StorageClient
    {
        $client = new StorageClient($params);
        $client->setRunId($this->getKbcRunId());
        return $client;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }

    private function getKbcRunId(): string
    {
        return (string) getenv('KBC_RUNID');
    }
}
