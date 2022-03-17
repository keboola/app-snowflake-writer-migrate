<?php

declare(strict_types=1);

namespace Keboola\AppSnowflakeWriterMigrate\Tests;

use GuzzleHttp\Client;
use GuzzleHttp\Psr7\Response;
use Keboola\AppSnowflakeWriterMigrate\Config;
use Keboola\AppSnowflakeWriterMigrate\MigrateWriter;
use Keboola\StorageApi\Options\Components\Configuration;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Workspaces;

class MigrateWriterTest extends TestCase
{
    public function testMigrateNonKeboolaWriter(): void
    {
        /** @var Components|MockObject $sourceComponentsApi */
        $sourceComponentsApi = $this->createMock(Components::class);

        /** @var Components|MockObject $destComponentsApi */
        $destComponentsApi = $this->createMock(Components::class);

        /** @var Workspaces|MockObject $destWorkspacesApi */
        $destWorkspacesApi = $this->createMock(Workspaces::class);

        /** @var Client|MockObject $encryptionApi */
        $encryptionApi = $this->createMock(Client::class);

        $sourceConfiguration = [
            'id' => '12',
            'name' => 'KBL',
            'description' => 'some desc',
            'created' => '2018-05-23T14:58:47+0200',
            'creatorToken' =>
                [
                    'id' => 142576,
                    'description' => 'martin.halamicek@keboola.com',
                ],
            'version' => 2,
            'changeDescription' => 'Update credentials',
            'isDeleted' => false,
            'configuration' =>
                [
                    'parameters' =>
                        [
                            'db' => [
                                'port' => '443',
                                'schema' => 'WORKSPACE_755797',
                                'warehouse' => 'KEBOOLA_PROD',
                                'driver' => 'snowflake',
                                'host' => 'random.snowflakecomputing.com',
                                'user' => 'SAPI_WORKSPACE_755797',
                                'database' => 'SAPI_4788',
                                'password' => 'secret',
                            ],
                            'tables' => [],
                        ],
                ],
            'rowsSortOrder' => [],
            'rows' => [],
            'state' => [],

        ];
        $sourceComponentsApi->expects($this->once())
            ->method('getConfiguration')
            ->with(
                Config::AWS_COMPONENT_ID,
                '12'
            )
            ->willReturn($sourceConfiguration);

        $destWorkspacesApi->expects($this->never())
            ->method('createWorkspace');

        $destComponentsApi->expects($this->once())
            ->method('addConfiguration')
            ->with($this->callback(function (Configuration $configuration) use ($sourceConfiguration) {
                $this->assertEquals($sourceConfiguration['id'], $configuration->getConfigurationId());
                $this->assertEquals(Config::AWS_COMPONENT_ID, $configuration->getComponentId());
                $this->assertEquals($sourceConfiguration['name'], $configuration->getName());
                $this->assertEquals($sourceConfiguration['description'], $configuration->getDescription());
                $this->assertEquals($sourceConfiguration['configuration'], $configuration->getConfiguration());
                return true;
            }));

        $migrateWriter = new MigrateWriter(
            $sourceComponentsApi,
            $destComponentsApi,
            $destWorkspacesApi,
            $encryptionApi
        );
        $migrateWriter->migrate('12');
    }

    public function testMigrateKeboolaSnowflakeWriter(): void
    {
        /** @var Components|MockObject $sourceComponentsApi */
        $sourceComponentsApi = $this->createMock(Components::class);

        /** @var Components|MockObject $destComponentsApi */
        $destComponentsApi = $this->createMock(Components::class);

        /** @var Workspaces|MockObject $destWorkspacesApi */
        $destWorkspacesApi = $this->createMock(Workspaces::class);

        /** @var Client|MockObject $encryptionApi */
        $encryptionApi = $this->createMock(Client::class);

        $sourceConfiguration = [
            'id' => '12',
            'name' => 'KBL',
            'description' => 'some desc',
            'created' => '2018-05-23T14:58:47+0200',
            'creatorToken' =>
                [
                    'id' => 142576,
                    'description' => 'martin.halamicek@keboola.com',
                ],
            'version' => 2,
            'changeDescription' => 'Update credentials',
            'isDeleted' => false,
            'configuration' =>
                [
                    'parameters' =>
                        [
                            'db' => [
                                'port' => '443',
                                'schema' => 'WORKSPACE_755797',
                                'warehouse' => 'KEBOOLA_PROD',
                                'driver' => 'snowflake',
                                'host' => 'keboola.snowflakecomputing.com',
                                'user' => 'SAPI_WORKSPACE_755797',
                                'database' => 'SAPI_47',
                                'password' => 'secret',
                            ],
                            'tables' => [],
                        ],
                ],
            'rowsSortOrder' => [],
            'rows' => [],
            'state' => [],

        ];
        $sourceComponentsApi->expects($this->once())
            ->method('getConfiguration')
            ->with(
                Config::AWS_COMPONENT_ID,
                '12'
            )
            ->willReturn($sourceConfiguration);

        $createdWorkspace = [
            'id' => 755885,
            'name' => 'WORKSPACE_755885',
            'component' => null,
            'configurationId' => null,
            'created' => '2018-05-23T15:42:01+0200',
            'connection' =>
                [
                    'backend' => 'snowflake',
                    'host' => 'keboola.snowflakecomputing.com',
                    'database' => 'SAPI_4788',
                    'schema' => 'WORKSPACE_755885',
                    'warehouse' => 'KEBOOLA_PROD',
                    'user' => 'SAPI_WORKSPACE_755885',
                    'password' => 'new-secret',
                ],
            'statementTimeoutSeconds' => 900,
            'creatorToken' =>
                [
                    'id' => 142576,
                    'description' => 'martin.halamicek@keboola.com',
                ],
        ];
        $destWorkspacesApi->expects($this->once())
            ->method('createWorkspace')
            ->with()
            ->willReturn($createdWorkspace);

        $expectedNewConfigurationData = [
            'parameters' => [
                'db' => [
                    'port' => '443',
                    'schema' => $createdWorkspace['connection']['schema'],
                    'warehouse' => $createdWorkspace['connection']['warehouse'],
                    'driver' => 'snowflake',
                    'host' => $createdWorkspace['connection']['host'],
                    'user' => $createdWorkspace['connection']['user'],
                    'database' => $createdWorkspace['connection']['database'],
                    'password' => $createdWorkspace['connection']['password'],
                    '#password' => 'encryptValue',
                ],
                'tables' => [],
            ],
        ];

        $destComponentsApi->expects($this->once())
            ->method('addConfiguration')
            ->with($this->callback(function (Configuration $configuration) use (
                $sourceConfiguration,
                $expectedNewConfigurationData
            ) {
                $this->assertEquals($sourceConfiguration['id'], $configuration->getConfigurationId());
                $this->assertEquals(Config::AWS_COMPONENT_ID, $configuration->getComponentId());
                $this->assertEquals($sourceConfiguration['name'], $configuration->getName());
                $this->assertEquals($sourceConfiguration['description'], $configuration->getDescription());
                $this->assertEquals($expectedNewConfigurationData, $configuration->getConfiguration());
                return true;
            }));

        $encryptionApi
            ->expects($this->once())
            ->method('send')
            ->willReturn(new Response(200, [], 'encryptValue'))
        ;

        $migrateWriter = new MigrateWriter(
            $sourceComponentsApi,
            $destComponentsApi,
            $destWorkspacesApi,
            $encryptionApi
        );
        $migrateWriter->migrate('12');
    }

    /**
     * @dataProvider isKeboolaWriterProvider
     * @param array $configurationData
     * @param bool $isKeboolaWriter
     */
    public function testIsKeboolaWriter(array $configurationData, bool $isKeboolaWriter): void
    {
        $this->assertEquals($isKeboolaWriter, MigrateWriter::isKeboolaProvisionedWriter($configurationData));
    }

    public function isKeboolaWriterProvider(): array
    {
        return [
            'empty' => [
                [],
                false,
            ],
            'empty-host' => [
                [
                    'parameters' => [
                        'db' => [
                            'host' => '',
                        ],
                    ],
                ],
                false,
            ],
            'client' => [
                [
                    'parameters' => [
                        'db' => [
                            'port' => '443',
                            'schema' => 'WORKSPACE_755797',
                            'warehouse' => 'KEBOOLA_PROD',
                            'driver' => 'snowflake',
                            'host' => 'customer.snowflakecomputing.com',
                            'user' => 'SAPI_WORKSPACE_755797',
                            'database' => 'SAPI_4788',
                            'password' => 'secret',
                        ],
                    ],
                ],
                false,
            ],
            'keboola-us' => [
                [
                    'parameters' => [
                        'db' => [
                            'port' => '443',
                            'schema' => 'WORKSPACE_755797',
                            'warehouse' => 'KEBOOLA_PROD',
                            'driver' => 'snowflake',
                            'host' => 'keboola.snowflakecomputing.com',
                            'user' => 'SAPI_WORKSPACE_755797',
                            'database' => 'SAPI_4788',
                            'password' => 'secret',
                        ],
                    ],
                ],
                true,
            ],
            'keboola-eu' => [
                [
                    'parameters' => [
                        'db' => [
                            'port' => '443',
                            'schema' => 'WORKSPACE_755797',
                            'warehouse' => 'KEBOOLA_PROD',
                            'driver' => 'snowflake',
                            'host' => 'keboola.eu-central-1.snowflakecomputing.com',
                            'user' => 'SAPI_WORKSPACE_755797',
                            'database' => 'SAPI_4788',
                            'password' => 'secret',
                        ],
                    ],
                ],
                true,
            ],
        ];
    }
}
