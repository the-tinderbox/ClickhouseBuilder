<?php

namespace Tinderbox\ClickhouseBuilder;

use Mockery as m;
use Illuminate\Config\Repository;
use Illuminate\Container\Container;
use Illuminate\Database\DatabaseServiceProvider;
use Illuminate\Events\EventServiceProvider;
use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Cluster;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;
use Tinderbox\ClickhouseBuilder\Exceptions\NotSupportedException;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\ClickhouseServiceProvider;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection;
use Tinderbox\ClickhouseBuilder\Query\Expression;

class LaravelIntegrationTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function getSimpleConfig()
    {
        return [
            'host' => 'localhost',
            'port' => 8123,
            'database' => 'database',
            'username' => 'default',
            'password' => '',
            'options' => [
                'timeout' => 10,
                'protocol' => 'http'
            ]
        ];
    }

    public function getClusterConfig()
    {
        return [
            'cluster' => [
                'server-1' => [
                    'host' => 'localhost',
                    'port' => 8123,
                    'database' => 'database',
                    'username' => 'default',
                    'password' => ''
                ],
                'server2' => [
                    'host' => 'localhost',
                    'port' => 8123,
                    'database' => 'database',
                    'username' => 'default',
                    'password' => ''
                ]
            ]
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
                            'driver' => 'clickhouse',
                            'host' => 'localhost',
                            'port' => 8123,
                            'database' => 'database',
                            'username' => 'default',
                            'password' => ''
                        ]
                    ]
                ]
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

        $simpleClient = $simpleConnection->getClient();
        $clusterClient = $clusterConnection->getClient();

        $this->assertInstanceOf(Cluster::class, $clusterClient->getCluster());
        $this->assertInstanceOf(Server::class, $simpleClient->getServer());
        $this->assertNull($simpleClient->getCluster());
    }

    public function test_connection_get_config()
    {
        $connection = new Connection($this->getSimpleConfig());

        $this->assertEquals($this->getSimpleConfig(), $connection->getConfig());
        $this->assertEquals('localhost', $connection->getConfig('host'));
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

    public function test_connection_begin_transaction()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->beginTransaction();
    }

    public function test_connection_select()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $result = m::mock(Result::class);
        $queryStat = m::mock(QueryStatistic::class);

        $queryStat->shouldReceive('getTime')->andReturn(10);
        $result->shouldReceive('getStatistic')->andReturn($queryStat);
        $result->shouldReceive('getRows')->andReturn([]);

        $client->shouldReceive('select')
            ->with('select * from `table`', [])
            ->andReturn($result);

        $connection->setClient($client);

        $connection->select('select * from `table`');
    }

    public function test_connection_select_one()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $result = m::mock(Result::class);
        $queryStat = m::mock(QueryStatistic::class);

        $queryStat->shouldReceive('getTime')->andReturn(10);
        $result->shouldReceive('getStatistic')->andReturn($queryStat);
        $result->shouldReceive('getRows')->andReturn([]);

        $client->shouldReceive('select')
            ->with('select * from `table`', [])
            ->andReturn($result);

        $connection->setClient($client);

        $connection->selectOne('select * from `table`');
    }

    public function test_connection_statement()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('statement')->with('query', [])->andReturn(true);
        $connection->setClient($client);

        $connection->statement('query');
    }

    public function test_connection_unprepared()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('statement')->with('query', [])->andReturn(true);
        $connection->setClient($client);

        $connection->unprepared('query');
    }

    public function test_connection_select_async()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);

        $result = m::mock(Result::class);
        $queryStat = m::mock(QueryStatistic::class);

        $queryStat->shouldReceive('getTime')->andReturn(10);
        $result->shouldReceive('getStatistic')->andReturn($queryStat);
        $result->shouldReceive('getRows')->andReturn([]);

        $client->shouldReceive('selectAsync')
            ->with([
                'select * from `table1`',
                'select * from `table2`'
            ])
            ->andReturn([$result]);
        $connection->setClient($client);

        $connection->selectAsync([
            'select * from `table1`',
            'select * from `table2`'
        ]);
    }

    public function test_connection_insert()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('insert')
        ->with('insert into `table` (`column`, `column2`) values (`val`, `val`)', [])
        ->andReturn(true);
        $connection->setClient($client);

        $result = $connection->insert('insert into `table` (`column`, `column2`) values (`val`, `val`)');

        $this->assertTrue($result);
    }

    public function test_connection_insert_files()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('insertFiles')
        ->with('table', ['column1', 'column2'], ['file1', 'file2'], null, 5)
        ->andReturn([]);
        $connection->setClient($client);

        $result = $connection->insertFiles('table', ['column1', 'column2'], ['file1', 'file2']);

        $this->assertEquals([], $result);
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

    public function test_connection_delete()
    {
        $connection = new Connection($this->getSimpleConfig());
        $this->expectException(NotSupportedException::class);
        $connection->delete('query');
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
        $connection->transaction(function () {});
    }

    public function test_connection_using()
    {
        $connection = new Connection($this->getClusterConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('using')->with('server-1')->andReturn($client);

        $connection->setClient($client);

        $connection->using('server-1');
    }

    public function test_builder_get()
    {
        $connection = new Connection($this->getClusterConfig());
        $client = m::mock(Client::class);
        $result = m::mock(Result::class);
        $queryStat = m::mock(QueryStatistic::class);

        $queryStat->shouldReceive('getTime')->andReturn(10);
        $result->shouldReceive('getStatistic')->andReturn($queryStat);
        $result->shouldReceive('getRows')->andReturn([]);

        $client->shouldReceive('select')->with('SELECT `column` FROM `table`', [])->andReturn($result);
        $connection->setClient($client);

        $connection->table('table')->select('column')->get();
    }

    public function test_builder_async_get()
    {
        $connection = new Connection($this->getClusterConfig());
        $client = m::mock(Client::class);
        $result = m::mock(Result::class);
        $queryStat = m::mock(QueryStatistic::class);

        $queryStat->shouldReceive('getTime')->andReturn(10);
        $result->shouldReceive('getStatistic')->andReturn($queryStat);
        $result->shouldReceive('getRows')->andReturn([]);

        $client->shouldReceive('selectAsync')->with(['SELECT * FROM `table`', 'SELECT * FROM `table2`'])->andReturn([$result]);
        $connection->setClient($client);

        $connection->table('table')->asyncWithQuery(function ($builder) {
            $builder->from('table2');
        })->get();
    }

    public function test_builder_insert_files()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('insertFiles')
            ->with('table', ['column1', 'column2'], ['file1', 'file2'], Format::CSV, 5)
            ->andReturn([]);
        $connection->setClient($client);

        $connection->table('table')->insertFiles(['column1', 'column2'], ['file1', 'file2']);
    }

    public function test_builder_insert()
    {
        $connection = new Connection($this->getSimpleConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('insert')
            ->with('INSERT INTO `table` (`column`, `column2`) FORMAT Values (?, ?)', ['val', 'val'])
            ->andReturn(true)->twice();
        $connection->setClient($client);

        $connection->table('table')->insert(['column' => 'val', 'column2' => 'val']);
        $connection->table('table')->insert([['column' => 'val', 'column2' => 'val']]);

        $client->shouldReceive('insert')
            ->with('INSERT INTO `table` FORMAT Values (?, ?)', ['val', 'val'])
            ->andReturn(true);

        $connection->table('table')->insert(['val', 'val']);

        $this->assertFalse($connection->table('table')->insert([]));
    }
}