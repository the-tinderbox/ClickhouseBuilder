<?php

namespace Tinderbox\ClickhouseBuilder;

use Mockery\Adapter\Phpunit\MockeryPHPUnitIntegration;
use Mockery as m;
use PHPUnit\Framework\TestCase;
use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Cluster;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\Clickhouse\Query\Result;
use Tinderbox\Clickhouse\Server;
use Tinderbox\ClickhouseBuilder\Exceptions\NotSupportedException;
use Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection;
use Tinderbox\ClickhouseBuilder\Query\Expression;

class LaravelIntegrationTest extends TestCase
{
    use MockeryPHPUnitIntegration;

    public function getSimpleConfig()
    {
        return [
            'host'     => 'localhost',
            'port'     => 8123,
            'database' => 'database',
            'username' => 'default',
            'password' => '',
            'options'  => [
                'timeout'  => 10,
                'protocol' => 'http',
            ],
        ];
    }

    public function getClusterConfig()
    {
        return [
            'cluster' => [
                'server-1' => [
                    'host'     => 'localhost',
                    'port'     => 8123,
                    'database' => 'database',
                    'username' => 'default',
                    'password' => '',
                ],
                'server2' => [
                    'host'     => 'localhost',
                    'port'     => 8123,
                    'database' => 'database',
                    'username' => 'default',
                    'password' => '',
                ],
            ],
        ];
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
            ->with('select * from `table`', [], [])
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
            ->with('select * from `table`', [], [])
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
        $connection->transaction(function () {
        });
    }

    public function test_connection_using()
    {
        $connection = new Connection($this->getClusterConfig());
        $client = m::mock(Client::class);
        $client->shouldReceive('using')->with('server-1')->andReturn($client);

        $connection->setClient($client);

        $connection->using('server-1');
    }
}
