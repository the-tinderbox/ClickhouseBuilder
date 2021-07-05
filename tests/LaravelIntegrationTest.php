<?php

namespace Tinderbox\ClickhouseBuilder;

use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Events\EventServiceProvider;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Common\FileFromString;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\ServerProvider;
use Tinderbox\Clickhouse\Transport\HttpTransport;
use Tinderbox\ClickhouseBuilder\Exceptions\BuilderException;
use Tinderbox\ClickhouseBuilder\Exceptions\NotSupportedException;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\ClickhouseServiceProvider;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection;
use Tinderbox\ClickhouseBuilder\Query\Expression;

class LaravelIntegrationTest extends TestCase
{
    public function getSimpleConfig()
    {
        return [
            'servers' => [
                [
                    'host'     => 'localhost',
                    'port'     => 8123,
                    'database' => 'default',
                    'username' => 'default',
                    'password' => '',
                    'options'  => [
                        'timeout'  => 10,
                        'protocol' => 'http',
                    ],
                ],
            ],
        ];
    }

    public function getClusterConfig()
    {
        return [
            'clusters' => [
                'test' => [
                    'server-1' => [
                        'host'     => 'localhost',
                        'port'     => 8123,
                        'database' => 'default',
                        'username' => 'default',
                        'password' => '',
                    ],
                    'server2'  => [
                        'host'     => 'localhost',
                        'port'     => 8123,
                        'database' => 'default',
                        'username' => 'default',
                        'password' => '',
                        'options'  => [
                            'timeout '=> 10,
                        ],
                    ],
                    'server3'  => [
                        'host'     => 'not_local_host',
                        'port'     => 8123,
                        'database' => 'default',
                        'username' => 'default',
                        'password' => '',
                        'options'  => [
                            'timeout' => 10,
                        ],
                    ],
                ],
            ],
        ];
    }

    public function getSimpleConfigWithServerWithTag()
    {
        return [
            'servers' => [
                [
                    'host'     => 'with-tag',
                    'port'     => 8123,
                    'database' => 'default',
                    'username' => 'default',
                    'password' => '',
                    'options'  => [
                        'tags' => [
                            'tag',
                        ],
                    ],
                ],
                [
                    'host'     => 'without-tag',
                    'port'     => 8123,
                    'database' => 'default',
                    'username' => 'default',
                    'password' => '',
                ],
            ],
        ];
    }

    public function getClusterConfigWithServerWithTag()
    {
        return [
            'clusters' => [
                'test' => [
                    [
                        'host'     => 'with-tag',
                        'port'     => 8123,
                        'database' => 'default',
                        'username' => 'default',
                        'password' => '',
                        'options'  => [
                            'tags' => [
                                'tag',
                            ],
                        ],
                    ],
                    [
                        'host'     => 'without-tag',
                        'port'     => 8123,
                        'database' => 'default',
                        'username' => 'default',
                        'password' => '',
                    ],
                ],
            ],
        ];
    }

    public function test_service_provider()
    {
        $clickHouseServiceProvider = new ClickhouseServiceProvider(Container::getInstance());
        $databaseServiceProvider = new DatabaseServiceProvider(Container::getInstance());
        $eventsServiceProvider = new EventServiceProvider(Container::getInstance());
        Container::getInstance()->singleton('config', function () {
            return new Repository([
                'database' => [
                    'connections' => [
                        'clickhouse' => [
                            'driver'   => 'clickhouse',
                            'host'     => 'localhost',
                            'port'     => 8123,
                            'database' => 'database',
                            'username' => 'default',
                            'password' => '',
                        ],
                    ],
                ],
            ]);
        });

        $eventsServiceProvider->register();
        $databaseServiceProvider->register();
        $databaseServiceProvider->boot();
        $clickHouseServiceProvider->boot();

        $database = Container::getInstance()->make('db')->connection('clickhouse');

        $this->assertInstanceOf(Connection::class, $database);
    }

    public function test_connection_construct()
    {
        $simpleConnection = new Connection($this->getSimpleConfig());
        $clusterConnection = new Connection($this->getClusterConfig());

        $clusterConnection->onCluster('test')->usingRandomServer();

        $simpleClient = $simpleConnection->getClient();
        $clusterClient = $clusterConnection->getClient();

        $clusterServer = $clusterClient->getServer();
        $secondClusterServer = $clusterClient->getServer();

        while ($secondClusterServer === $clusterServer) {
            $secondClusterServer = $clusterClient->getServer();
        }

        $this->assertNotSame($clusterServer, $secondClusterServer);
        $this->assertEquals($simpleClient->getServer(), $simpleClient->getServer());
    }

    public function test_connection_with_server_with_tags()
    {
        $simpleConnection = new Connection($this->getSimpleConfigWithServerWithTag());
        $clusterConnection = new Connection($this->getClusterConfigWithServerWithTag());

        $simpleConnection->usingServerWithTag('tag');
        $clusterConnection->onCluster('test')->usingServerWithTag('tag');

        $simpleServerWithTag = $simpleConnection->getClient()->getServer();
        $clusterServerWithTag = $clusterConnection->getClient()->getServer();

        $simpleConnection->usingRandomServer();
        $clusterConnection->onCluster('test')->usingRandomServer();

        $simpleServer = $simpleConnection->getClient()->getServer();
        $clusterServer = $clusterConnection->getClient()->getServer();

        while ($simpleServer === $simpleServerWithTag) {
            $simpleServer = $simpleConnection->getClient()->getServer();
        }

        while ($clusterServer === $simpleServerWithTag) {
            $clusterServer = $clusterConnection->getClient()->getServer();
        }

        $this->assertTrue(true);
        $this->assertEquals('with-tag', $simpleServerWithTag->getHost());
        $this->assertEquals('with-tag', $clusterServerWithTag->getHost());
    }

    public function test_connection_get_config()
    {
        $connection = new Connection($this->getSimpleConfig());

        $this->assertEquals($this->getSimpleConfig(), $connection->getConfig());
    }

    public function test_connection_query()
    {
        $connection = new Connection($this->getSimpleConfig());

        $this->assertInstanceOf(Builder::class, $connection->query());
    }

    public function test_connection_table()
    {
        $connection = new Connection($this->getSimpleConfig());
        $builder = $connection->table('table');

        $this->assertInstanceOf(Builder::class, $builder);
        $this->assertEquals('SELECT * FROM `table`', $builder->toSql());
    }

    public function test_connection_raw()
    {
        $connection = new Connection($this->getSimpleConfig());

        $this->assertInstanceOf(Expression::class, $connection->raw('value'));
    }

    public function test_connection_select()
    {
        $connection = new Connection($this->getSimpleConfig());

        $result = $connection->select('select * from numbers(0, 10)');
        $this->assertEquals(10, count($result));
    }

    public function test_connection_select_one()
    {
        $connection = new Connection($this->getSimpleConfig());

        $result = $connection->selectOne('select * from numbers(0, 10)');
        $this->assertEquals(10, count($result));
    }

    public function test_connection_statement()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->statement('drop table if exists test');

        $result = $connection->select("select count() as count from system.tables where name = 'test'");
        $this->assertEquals(0, $result[0]['count']);

        $connection->statement('create table test (test String) Engine = Memory');

        $result = $connection->select("select count() as count from system.tables where name = 'test'");
        $this->assertEquals(1, $result[0]['count']);

        $connection->statement('drop table if exists test');
        $result = $connection->select("select count() as count from system.tables where name = 'test'");
        $this->assertEquals(0, $result[0]['count']);
    }

    public function test_connection_unprepared()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->unprepared('drop table if exists test');

        $result = $connection->select('select count() as count from system.tables where name = \'test\'');
        $this->assertEquals(0, $result[0]['count']);

        $connection->statement('create table test (test String) Engine = Memory');

        $result = $connection->select('select count() as count from system.tables where name = \'test\'');
        $this->assertEquals(1, $result[0]['count']);

        $connection->statement('drop table if exists test');
        $result = $connection->select('select count() as count from system.tables where name = \'test\'');
        $this->assertEquals(0, $result[0]['count']);
    }

    public function test_connection_select_async()
    {
        $connection = new Connection($this->getSimpleConfig());

        $result = $connection->selectAsync([
            ['query' => 'select * from numbers(0, 10)'],
            ['query' => 'select * from numbers(10, 10)'],
        ]);

        $this->assertEquals(2, count($result));
        $this->assertEquals(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], array_column($result[0], 'number'));
        $this->assertEquals(['10', '11', '12', '13', '14', '15', '16', '17', '18', '19'], array_column($result[1], 'number'));
    }

    public function test_connection_insert()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->statement('drop table if exists test');
        $connection->statement('create table test (number UInt64) engine = Memory');

        $result = $connection->insert('insert into test (number) values (?), (?), (?)', [0, 1, 2]);
        $this->assertTrue($result);

        $result = $connection->select('select * from test');

        $this->assertEquals(3, count($result));
    }

    public function test_connection_insert_files()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->statement('drop table if exists test');
        $connection->statement('create table test (number UInt64) engine = Memory');

        $result = $connection->insertFiles('test', ['number'], [
            new FileFromString('0'.PHP_EOL.'1'.PHP_EOL.'2'),
        ]);
        $this->assertTrue($result[0][0]);

        $result = $connection->select('select * from test');

        $this->assertEquals(3, count($result));
    }

    /*
     * Not supported functions
     */

    public function test_connection_begin_transaction()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->beginTransaction();
    }

    public function test_connection_update()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->update('query');
    }

    public function test_connection_commit()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->commit();
    }

    public function test_last_query_statistic()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->table($connection->raw('numbers(0,10)'))->select('number')->get();

        $firstStatistic = $connection->getLastQueryStatistic();

        $connection->table($connection->raw('numbers(0,10000)'))->select('number')->get();

        $secondStatistic = $connection->getLastQueryStatistic();

        $this->assertNotSame($firstStatistic, $secondStatistic);

        $this->expectException(BuilderException::class);
        $this->expectExceptionMessage('Run query before trying to get statistic');

        $connection = new Connection($this->getSimpleConfig());
        $connection->getLastQueryStatistic();
    }

    public function test_connection_delete()
    {
        /*
         * delete redirects call to statement method, so
         * just test it like statement
         */
        $connection = new Connection($this->getSimpleConfig());
        $connection->delete('drop table if exists test');

        $result = $connection->select("select count() as count from system.tables where name = 'test'");
        $this->assertEquals(0, $result[0]['count']);

        $connection->delete('create table test (test String) Engine = Memory');

        $result = $connection->select("select count() as count from system.tables where name = 'test'");
        $this->assertEquals(1, $result[0]['count']);

        $connection->delete('drop table if exists test');
        $result = $connection->select("select count() as count from system.tables where name = 'test'");
        $this->assertEquals(0, $result[0]['count']);
    }

    public function test_connection_affecting_statement()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->affectingStatement('query');
    }

    public function test_connection_rollback()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->rollBack();
    }

    public function test_connection_transaction_level()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->transactionLevel();
    }

    public function test_connection_transaction()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->transaction(function () {
        });
    }

    public function test_connection_using()
    {
        $connection = new Connection($this->getClusterConfig());

        $connection->onCluster('test')->using('server-1')->statement('drop table if exists test1');
        $connection->onCluster('test')->using('server2')->statement('drop table if exists test2');

        $connection->onCluster('test')->using('server-1')->statement('create database if not exists cluster1');
        $connection->onCluster('test')->using('server2')->statement('create database if not exists cluster2');

        $connection->onCluster('test')->using('server-1')->statement('create table test1 (number UInt8) Engine = Memory');
        $connection->onCluster('test')->using('server2')->statement('create table test2 (number UInt8) Engine = Memory');

        $result = $connection->onCluster('test')->using('server-1')->insert('insert into test1 (number) values (?), (?), (?)', [0, 1, 2]);
        $this->assertTrue($result);

        $result = $connection->select('select * from test1');

        $this->assertEquals(3, count($result));

        $result = $connection->onCluster('test')->using('server2')->insert('insert into test2 (number) values (?), (?), (?), (?)', [0, 1, 2, 4]);
        $this->assertTrue($result);

        $result = $connection->select('select * from test2');

        $this->assertEquals(4, count($result));

        $connection->onCluster('test')->using('server-1')->statement('drop table if exists test1');
        $connection->onCluster('test')->using('server2')->statement('drop table if exists test2');
    }

    public function test_builder_get()
    {
        $connection = new Connection($this->getSimpleConfig());

        $result = $connection->table($connection->raw('numbers(0,10)'))->select('number')->get();

        $this->assertEquals(10, count($result));
    }

    public function test_builder_async_get()
    {
        $connection = new Connection($this->getSimpleConfig());
        $result = $connection->table($connection->raw('numbers(0,10)'))->select('number')->asyncWithQuery(function ($builder) use ($connection) {
            $builder->table($connection->raw('numbers(10,10)'))->select('number');
        })->get();

        $this->assertEquals(2, count($result));
        $this->assertEquals(['0', '1', '2', '3', '4', '5', '6', '7', '8', '9'], array_column($result[0], 'number'));
        $this->assertEquals(['10', '11', '12', '13', '14', '15', '16', '17', '18', '19'], array_column($result[1], 'number'));
    }

    public function test_builder_insert_files()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->statement('drop table if exists test');
        $connection->statement('create table test (number UInt64) engine = Memory');

        $result = $connection->table('test')->insertFiles(['number'], [
            new FileFromString('0'.PHP_EOL.'1'.PHP_EOL.'2'),
        ]);
        $this->assertTrue($result[0][0]);

        $result = $connection->table('test')->get();

        $this->assertEquals(3, count($result));

        $connection->statement('drop table if exists test');
        $connection->statement('create table test (number UInt64) engine = Memory');

        $result = $connection->table('test')->insertFile(['number'], new FileFromString('0'.PHP_EOL.'1'.PHP_EOL.'2'));
        $this->assertTrue($result);

        $result = $connection->table('test')->get();

        $this->assertEquals(3, count($result));
    }

    public function test_builder_insert()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->statement('drop table if exists test');
        $connection->statement('create table test (number UInt64) engine = Memory');

        $connection->table('test')->insert(['number' => 1]);
        $connection->table('test')->insert([['number' => 2], ['number' => 3]]);

        $connection->table('test')->insert([4]);
        $connection->table('test')->insert([[5], [6]]);

        $result = $connection->table('test')->select('number')->get();
        $this->assertEquals(6, count($result));

        $this->assertFalse($connection->table('table')->insert([]));
    }

    public function test_builder_delete()
    {
        $connection = new Connection($this->getSimpleConfig());
        $connection->statement('drop table if exists test');
        $connection->statement('create table test (number UInt64) engine = MergeTree order by number');

        $connection->table('test')->insertFiles(['number'], [
            new FileFromString('0'.PHP_EOL.'1'.PHP_EOL.'2'),
        ]);

        $result = $connection->table('test')->select($connection->raw('count() as count'))->get();

        $this->assertEquals(3, $result[0]['count']);

        $connection->table('test')->where('number', '=', 1)->delete();

        /*
         * We have to sleep for 3 seconds while mutation in progress
         */
        sleep(3);

        $result = $connection->table('test')->select($connection->raw('count() as count'))->get();

        $this->assertEquals(2, $result[0]['count']);
    }

    public function test_builder_count()
    {
        $connection = new Connection($this->getSimpleConfig());
        $result = $connection->table($connection->raw('numbers(0,10)'))->count();

        $this->assertEquals(10, $result);

        $result = $connection->table($connection->raw('numbers(0,10)'))->groupBy($connection->raw('number % 2'))->count();

        $this->assertEquals(2, $result);
    }

    public function test_builder_first()
    {
        $connection = new Connection($this->getSimpleConfig());
        $result = $connection->table($connection->raw('numbers(2,10)'))->first();

        $this->assertEquals(2, $result['number']);
    }

    public function test_builder_connection()
    {
        $connection = new Connection($this->getSimpleConfig());
        $builder = $connection->table($connection->raw('numbers(2,10)'));

        $this->assertEquals($connection, $builder->getConnection());
    }

    public function test_builder_pagination()
    {
        $connection = new Connection($this->getSimpleConfig());
        $paginator1 = $connection->table($connection->raw('numbers(0,10)'))->paginate(1, 2);

        $this->assertEquals(2, $paginator1->count());
        $this->assertEquals(2, $paginator1->perPage());
        $this->assertEquals(1, $paginator1->currentPage());
        $this->assertEquals(5, $paginator1->lastPage());
        $this->assertEquals([['number' => 0], ['number' => 1]], $paginator1->values()->all());

        $paginator2 = $connection->table($connection->raw('numbers(0,10)'))->paginate(3, 1);

        $this->assertEquals(1, $paginator2->count());
        $this->assertEquals(1, $paginator2->perPage());
        $this->assertEquals(3, $paginator2->currentPage());
        $this->assertEquals(10, $paginator2->lastPage());
        $this->assertEquals([['number' => 2]], $paginator2->values()->all());
    }

    public function test_connection_set_client()
    {
        $connection = new Connection($this->getSimpleConfig());

        $server = new Server('127.0.0.2');
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $transport = new HttpTransport(new \GuzzleHttp\Client());

        $client = new Client($serverProvider, $transport);
        $connection->setClient($client);

        $this->assertEquals($client, $connection->getClient());
    }

    public function test_builder_last_query_statistics()
    {
        $connection = new Connection($this->getSimpleConfig());

        $server = new Server('127.0.0.1');
        $serverProvider = new ServerProvider();
        $serverProvider->addServer($server);

        $transport = $this->createMock(TransportInterface::class);
        $transport->method('read')->willReturn([
            new Result(new Query($server, ''), [0, 1], new QueryStatistic(10, 20, 30, 40)),
        ]);

        $client = new Client($serverProvider, $transport);

        $connection->setClient($client);

        $builder = $connection->table('test');
        $builder->get();

        $lastQueryStatistic = $builder->getLastQueryStatistics();

        $this->assertEquals(10, $lastQueryStatistic->getRows());
        $this->assertEquals(20, $lastQueryStatistic->getBytes());
        $this->assertEquals(30, $lastQueryStatistic->getTime());
        $this->assertEquals(40, $lastQueryStatistic->getRowsBeforeLimitAtLeast());
    }
}
