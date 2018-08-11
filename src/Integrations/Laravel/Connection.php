<?php

namespace Tinderbox\ClickhouseBuilder\Integrations\Laravel;

use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Cluster;
use Tinderbox\Clickhouse\Common\ServerOptions;
use Tinderbox\Clickhouse\Interfaces\TransportInterface;
use Tinderbox\Clickhouse\Server;
use Tinderbox\Clickhouse\Transport\ClickhouseCLIClientTransport;
use Tinderbox\Clickhouse\Transport\HttpTransport;
use Tinderbox\ClickhouseBuilder\Exceptions\NotSupportedException;
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
     * Create a new database connection instance.
     *
     * Config should be like this structure for server:
     *
     * $config = [
     *      'host' => '',
     *      'port' => '',
     *      'database' => '',
     *      'username' => '',
     *      'password' => '',
     *      'options' => [
     *          'timeout' => 10,
     *          'protocol' => 'https'
     *      ],
     *      'transport' => 'cli',
     *      'transportOptions' => [
     *          'executable' => '/usr/bin/clickhouse-client'
     *      ]
     * ];
     *
     * And like this structure for cluster:
     *
     * $config = [
     *      'cluster' => [
     *          'server-1' => [
     *              'host' => '',
     *              'port' => '',
     *              'database' => '',
     *              'username' => '',
     *              'password' => '',
     *              'options' => [
     *                  'timeout' => 10,
     *                  'protocol' => 'https'
     *              ]
     *          ],
     *
     *          'server-2' => [
     *              'host' => '',
     *              'port' => '',
     *              'database' => '',
     *              'username' => '',
     *              'password' => '',
     *              'options' => [
     *                  'timeout' => 10,
     *                  'protocol' => 'https'
     *              ]
     *          ]
     *      ]
     * ];
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        $this->config = $config;

        $server = $this->assembleClientServer($config);

        $transport = $this->createTransport($config['transport'] ?? 'http', $config['transportOptions'] ?? []);
        $this->client = $this->createClientFor($server, $transport);

        if (isset($config['random_server']) && $config['random_server'] === true) {
            $this->client->useRandomServer(true);
        }
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
     * Creates Clickhouse client.
     *
     * @param mixed              $server
     * @param TransportInterface $transport
     *
     * @return Client
     */
    protected function createClientFor($server, TransportInterface $transport)
    {
        return new Client($server, null, $transport);
    }

    /**
     * Creates transport
     *
     * @param string $transport
     * @param array  $options
     *
     * @return \Tinderbox\Clickhouse\Interfaces\TransportInterface
     */
    protected function createTransport(string $transport, array $options) : TransportInterface
    {
        switch ($transport) {
            case 'http':
                return new HttpTransport();
                break;

            case 'cli':
                return new ClickhouseCLIClientTransport($options['executable'] ?? null);
                break;
        }
    }

    /**
     * Assemble Server or Cluster depends on given config.
     *
     * @param array $config
     *
     * @return Cluster|Server
     */
    protected function assembleClientServer(array $config)
    {
        if (isset($config['cluster'])) {
            $cluster = new Cluster();

            foreach ($config['cluster'] as $hostname => $server) {
                $cluster->addServer($hostname, $this->assembleServer($server));
            }

            return $cluster;
        } else {
            return $this->assembleServer($config);
        }
    }

    /**
     * Assemble Server instance from array.
     *
     * @param array $server
     *
     * @return Server
     */
    protected function assembleServer(array $server) : Server
    {
        /* @var string $host */
        /* @var string $port */
        /* @var string $database */
        /* @var string $username */
        /* @var string $password */
        /* @var array $options */
        extract($server);

        if (isset($options)) {
            $timeout = $options['timeout'] ?? null;
            $protocol = $options['protocol'] ?? null;

            $options = new ServerOptions();

            if (! is_null($timeout)) {
                $options->setTimeout($timeout);
            }

            if (! is_null($protocol)) {
                $options->setProtocol($protocol);
            }
        }

        return new Server($host, $port ?? null, $database ?? null, $username ?? null, $password ?? null, $options ?? null);
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
     * @param string $table
     *
     * @return \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Builder
     */
    public function table($table)
    {
        return $this->query()->from($table);
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
     * Returns Clickhouse client.
     *
     * @return Client
     */
    public function getClient() : Client
    {
        return $this->client;
    }

    /**
     * Set client to the connection.
     *
     * @param Client $client
     *
     * @return Connection
     */
    public function setClient(Client $client) : self
    {
        $this->client = $client;

        return $this;
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
        $result = $this->getClient()->select($query, $bindings, $tables);

        $this->logQuery($query, $bindings, $result->getStatistic()->getTime());

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
        $queriesKeys = array_keys($queries);
        $results = array_combine($queriesKeys, $this->getClient()->selectAsync($queries));

        foreach ($results as $i => $result) {
            /* @var \Tinderbox\Clickhouse\Query\Result $result */
            $query = $queries[$i][0];
            $bindings = $queries[$i][1] ?? [];

            $this->logQuery($query, $bindings, $result->getStatistic()->getTime());

            $results[$i] = $result->getRows();
        }

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
     *
     * @return int
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

        $result = $this->getClient()->insert($query, $bindings);

        $this->logQuery($query, $bindings, microtime(true) - $startTime);

        return $result;
    }

    /**
     * Run async insert queries from local CSV or TSV files.
     *
     * @param string $table
     * @param array  $columns
     * @param array  $files
     * @param null   $format
     * @param int    $concurrency
     *
     * @return array
     */
    public function insertFiles($table, array $columns, array $files, $format = null, $concurrency = 5)
    {
        $result = $this->getClient()->insertFiles($table, $columns, $files, $format, $concurrency);

        $this->logQuery("INSERT FILES INTO {$table}", $files);

        return $result;
    }

    /**
     * Run an update statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @throws NotSupportedException
     *
     * @return int
     */
    public function update($query, $bindings = [])
    {
        throw NotSupportedException::updateAndDelete();
    }

    /**
     * Run a delete statement against the database.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @throws NotSupportedException
     *
     * @return int
     */
    public function delete($query, $bindings = [])
    {
        throw NotSupportedException::updateAndDelete();
    }

    /**
     * Run an SQL statement and get the number of rows affected.
     *
     * @param string $query
     * @param array  $bindings
     *
     * @throws NotSupportedException
     *
     * @return int
     */
    public function affectingStatement($query, $bindings = [])
    {
        throw NotSupportedException::updateAndDelete();
    }

    /**
     * Run a select statement and return a single result.
     *
     * @param string $query
     * @param array  $bindings
     * @param bool   $useReadPdo
     *
     * @return mixed
     */
    public function selectOne($query, $bindings = [], $useReadPdo = true)
    {
        $start = microtime(true);

        $result = $this->select($query, $bindings);

        $this->logQuery($query, $bindings, microtime(true) - $start);

        return array_shift($result);
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

        $result = $this->getClient()->statement($query, $bindings);

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
    public function using(string $hostname) : self
    {
        $this->getClient()->using($hostname);

        return $this;
    }
}
