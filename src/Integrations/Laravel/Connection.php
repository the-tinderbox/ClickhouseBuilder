<?php

namespace Tinderbox\ClickhouseBuilder\Integrations\Laravel;

use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Cluster;
use Tinderbox\Clickhouse\Common\ServerOptions;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Query;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\ServerProvider;
use Tinderbox\Clickhouse\Transport\HttpTransport;
use Tinderbox\ClickhouseBuilder\Exceptions\BuilderException;
use Tinderbox\ClickhouseBuilder\Exceptions\NotSupportedException;
use Tinderbox\ClickhouseBuilder\Query\Enums\Format;
use Tinderbox\ClickhouseBuilder\Query\Expression;

class Connection extends \Illuminate\Database\Connection
{
    /**
     * Clickhouse connection handler.
     *
     * @var Client
     */
    protected $client;

    /**
     * Given config.
     *
     * @var array
     */
    protected $config;

    /**
     * All of the queries run against the connection.
     *
     * @var array
     */
    protected $queryLog = [];

    /**
     * Indicates whether queries are being logged.
     *
     * @var bool
     */
    protected $loggingQueries = false;

    /**
     * The event dispatcher instance.
     *
     * @var \Illuminate\Contracts\Events\Dispatcher
     */
    protected $events;

    /**
     * Indicates if the connection is in a "dry run".
     *
     * @var bool
     */
    protected $pretending = false;

    /**
     * Last executed query statistic.
     *
     * @var \Tinderbox\Clickhouse\Query\QueryStatistic
     */
    protected $lastQueryStatistic;

    /**
     * Create a new database connection instance.
     *
     * Config should be like this structure for server:
     *
     * @param array $config
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClusterException
     * @throws \Tinderbox\Clickhouse\Exceptions\ServerProviderException
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        $serverProvider = $this->assembleServerProvider($config);

        $transport = $this->createTransport($config['transportOptions'] ?? []);
        $this->client = $this->createClientFor($serverProvider, $transport);
    }

    /**
     * Returns given config.
     *
     * @param mixed $option
     *
     * @return array
     */
    public function getConfig($option = null)
    {
        if (is_null($option)) {
            return $this->config;
        }

        return $this->config[$option] ?? null;
    }

    /**
     * Returns statistic for last query.
     *
     * @throws \Tinderbox\ClickhouseBuilder\Exceptions\BuilderException
     *
     * @return array|\Tinderbox\Clickhouse\Query\QueryStatistic
     */
    public function getLastQueryStatistic()
    {
        if (is_null($this->lastQueryStatistic)) {
            throw new BuilderException('Run query before trying to get statistic');
        }

        return $this->lastQueryStatistic;
    }

    /**
     * Sets last query statistic.
     *
     * @param array|\Tinderbox\Clickhouse\Query\QueryStatistic $queryStatistic
     */
    protected function setLastQueryStatistic($queryStatistic)
    {
        $this->lastQueryStatistic = $queryStatistic;
    }

    /**
     * Creates Clickhouse client.
     *
     * @param mixed              $server
     * @param TransportInterface $transport
     *
     * @return Client
     */
    protected function createClientFor($server, TransportInterface $transport)
    {
        return new Client($server, $transport);
    }

    /**
     * Creates transport.
     *
     * @param array $options
     *
     * @return \Tinderbox\Clickhouse\Interfaces\TransportInterface
     */
    protected function createTransport(array $options): TransportInterface
    {
        $client = $options['client'] ?? null;

        unset($options['client']);

        return new HttpTransport($client, $options);
    }

    /**
     * Assemble ServerProvider.
     *
     * @param array $config
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClusterException
     * @throws \Tinderbox\Clickhouse\Exceptions\ServerProviderException
     *
     * @return ServerProvider
     */
    protected function assembleServerProvider(array $config)
    {
        $serverProvider = new ServerProvider();

        if (empty($config['clusters'] ?? []) && empty($config['servers'] ?? [])) {
            $serverProvider->addServer($this->assembleServer($config));

            return $serverProvider;
        }

        foreach ($config['clusters'] ?? [] as $clusterName => $servers) {
            $cluster = new Cluster(
                $clusterName,
                array_map(
                    function ($server) {
                        return $this->assembleServer($server);
                    },
                    $servers
                )
            );

            $serverProvider->addCluster($cluster);
        }

        foreach ($config['servers'] ?? [] as $server) {
            $serverProvider->addServer($this->assembleServer($server));
        }

        return $serverProvider;
    }

    /**
     * Assemble Server instance from array.
     *
     * @param array $server
     *
     * @return Server
     */
    protected function assembleServer(array $server): Server
    {
        /* @var string $host */
        /* @var string $port */
        /* @var string $database */
        /* @var string $username */
        /* @var string $password */
        /* @var array $options */
        extract($server);

        if (isset($options)) {
            $protocol = $options['protocol'] ?? null;
            $tags = $options['tags'] ?? [];

            $options = new ServerOptions();

            if (!is_null($protocol)) {
                $options->setProtocol($protocol);
            }

            if (is_array($tags) && !empty($tags)) {
                foreach ($tags as $tag) {
                    $options->addTag($tag);
                }
            }
        }

        return new Server(
            $host,
            $port ?? null,
            $database ?? null,
            $username ?? null,
            $password ?? null,
            $options ?? null
        );
    }

    /**
     * Get a new query builder instance.
     *
     * @return \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder
     */
    public function query()
    {
        return new Builder($this);
    }

    /**
     * Begin a fluent query against a database table.
     *
     * @param \Closure|Builder|string $table
     * @param string|null             $as
     *
     * @return \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder
     */
    public function table($table, $as = null)
    {
        return $this->query()->from($table, $as);
    }

    /**
     * Get a new raw query expression.
     *
     * @param mixed $value
     *
     * @return Expression
     */
    public function raw($value)
    {
        return new Expression($value);
    }

    /**
     * Start a new database transaction.
     *
     * @throws \Exception
     *
     * @return void
     */
    public function beginTransaction()
    {
        throw NotSupportedException::transactions();
    }

    /**
     * Sets Clickhouse client.
     *
     * @var Client
     *
     * @return self
     */
    public function setClient(Client $client)
    {
        $this->client = $client;

        return $this;
    }

    /**
     * Returns Clickhouse client.
     *
     * @return Client
     */
    public function getClient(): Client
    {
        return $this->client;
    }

    /**
     * Run a select statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     * @param array  $tables
     *
     * @return array
     */
    public function select($query, $bindings = [], $tables = [])
    {
        $result = $this->getClient()->readOne($query, $tables);

        $this->logQuery($result->getQuery()->getQuery(), [], $result->getStatistic()->getTime());

        $this->setLastQueryStatistic($result->getStatistic());

        return $result->getRows();
    }

    /**
     * Run a select statements in async mode.
     *
     * @param array $queries
     *
     * @return array of results for each query
     */
    public function selectAsync(array $queries)
    {
        foreach ($queries as $i => $query) {
            if (method_exists($query, 'toQuery')) {
                $queries[$i] = $query->toQuery();
            }
        }

        $results = $this->getClient()->read($queries);
        $statistic = [];

        foreach ($results as $i => $result) {
            /* @var \Tinderbox\Clickhouse\Query\Result $result */
            /* @var Query $query */
            $query = $result->getQuery();

            $this->logQuery($query->getQuery(), [], $result->getStatistic()->getTime());

            $results[$i] = $result->getRows();
            $statistic[$i] = $result->getStatistic();
        }

        $this->setLastQueryStatistic($statistic);

        return $results;
    }

    /**
     * Commit the active database transaction.
     *
     * @throws NotSupportedException
     *
     * @return void
     */
    public function commit()
    {
        throw NotSupportedException::transactions();
    }

    /**
     * Rollback the active database transaction.
     *
     * @param null $toLevel
     *
     * @throws NotSupportedException
     */
    public function rollBack($toLevel = null)
    {
        throw NotSupportedException::transactions();
    }

    /**
     * Get the number of active transactions.
     *
     * @throws NotSupportedException
     */
    public function transactionLevel()
    {
        throw NotSupportedException::transactions();
    }

    /**
     * Execute a Closure within a transaction.
     *
     * @param \Closure $callback
     * @param int      $attempts
     *
     * @throws \Throwable
     *
     * @return mixed
     */
    public function transaction(\Closure $callback, $attempts = 1)
    {
        throw NotSupportedException::transactions();
    }

    /**
     * Run an insert statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return bool
     */
    public function insert($query, $bindings = [])
    {
        $startTime = microtime(true);

        $result = $this->getClient()->writeOne($query);

        $this->logQuery($query, $bindings, microtime(true) - $startTime);

        return $result;
    }

    /**
     * Run async insert queries from local CSV or TSV files.
     *
     * @param string      $table
     * @param array       $columns
     * @param array       $files
     * @param null|string $format
     * @param int         $concurrency
     *
     * @return array
     */
    public function insertFiles($table, array $columns, array $files, $format = Format::CSV, $concurrency = 5)
    {
        $result = $this->getClient()->writeFiles($table, $columns, $files, $format, [], $concurrency);

        $this->logQuery('INSERT '.count($files)." FILES INTO {$table}", []);

        return $result;
    }

    /**
     * Run an update statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @throws NotSupportedException
     */
    public function update($query, $bindings = [])
    {
        throw NotSupportedException::update();
    }

    /**
     * Run a delete statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return int
     */
    public function delete($query, $bindings = [])
    {
        return $this->statement($query);
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @throws NotSupportedException
     */
    public function affectingStatement($query, $bindings = [])
    {
        throw new NotSupportedException('This type of queries is not supported');
    }

    /**
     * Run a select statement and return a single result.
     *
     * @param string $query
     * @param array  $bindings
     * @param array  $tables
     *
     * @return mixed
     */
    public function selectOne($query, $bindings = [], $tables = [])
    {
        return $this->select($query, $bindings, $tables);
    }

    /**
     * Execute an SQL statement and return the boolean result.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @return bool
     */
    public function statement($query, $bindings = [])
    {
        $start = microtime(true);

        $result = $this->getClient()->writeOne($query);

        $this->logQuery($query, $bindings, microtime(true) - $start);

        return $result;
    }

    /**
     * Run a raw, unprepared query against the PDO connection.
     *
     * @param string $query
     *
     * @return bool
     */
    public function unprepared($query)
    {
        return $this->statement($query);
    }

    /**
     * Choose server to perform queries.
     *
     * @param string $hostname
     *
     * @return \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection
     */
    public function using(string $hostname): self
    {
        $this->getClient()->using($hostname);

        return $this;
    }

    /**
     * Choose cluster to perform queries.
     *
     * @param string|null $clusterName
     *
     * @return Connection
     */
    public function onCluster(?string $clusterName): self
    {
        $this->getClient()->onCluster($clusterName);

        return $this;
    }

    /**
     * Choose random server for each query.
     *
     * @return Connection
     */
    public function usingRandomServer(): self
    {
        $this->getClient()->usingRandomServer();

        return $this;
    }

    /**
     * Choose server with tag for queries.
     *
     * @param string $tag
     *
     * @return Connection
     */
    public function usingServerWithTag(string $tag): self
    {
        $this->getClient()->usingServerWithTag($tag);

        return $this;
    }

    /**
     * Returns server on which query will be executed.
     *
     * @return Server
     */
    public function getServer(): Server
    {
        return $this->getClient()->getServer();
    }
}
