<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate;

use Exception;
use GuzzleHttp\Client as GuzzleClient;
use Keboola\Component\BaseComponent;
use Keboola\StorageApi\Client;
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
        $destVerifyToken = $destProjectClient->verifyToken();

        $logger->info(sprintf(
            'Migrating Snowflake writers from project %s (%d) at %s',
            $sourceTokenInfo['owner']['name'],
            $sourceTokenInfo['owner']['id'],
            $config->getSourceProjectUrl()
        ));

        $sourceProjectComponentsApi = new Components($sourceProjectClient);

        $encryptionClient = new GuzzleClient([
            'base_uri' => $this->getServiceUrl($destProjectClient, 'encryption'),
        ]);

        $targetComponentId = $config->getImageParameters()['componentId'];
        if (in_array($targetComponentId, [Config::GCP_S3_COMPONENT_ID, Config::GCP_COMPONENT_ID])) {
            $targetComponentId = $destVerifyToken['owner']['defaultBackend'] === 'snowflake' ?
                Config::GCP_S3_COMPONENT_ID :
                Config::GCP_COMPONENT_ID;
        }

        foreach ($config->getSourceComponentId() as $writerComponentId) {
            $migrate = new MigrateWriter(
                $sourceProjectComponentsApi,
                new Components($destProjectClient),
                new Workspaces($destProjectClient),
                $encryptionClient,
                $logger,
                $destVerifyToken,
                $writerComponentId,
                $targetComponentId,
                $config->isDryRun()
            );
            $writers = $sourceProjectComponentsApi->listComponentConfigurations(
                (new ListComponentConfigurationsOptions())
                    ->setComponentId($writerComponentId)
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

    private function getServiceUrl(Client $client, string $serviceId): string
    {
        $services = $client->indexAction()['services'];

        $foundServices = array_values(array_filter($services, function ($service) use ($serviceId) {
            return $service['id'] === $serviceId;
        }));

        if (empty($foundServices)) {
            throw new Exception(sprintf('%s service not found', $serviceId));
        }

        return $foundServices[0]['url'];
    }
}
