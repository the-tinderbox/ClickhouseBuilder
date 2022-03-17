<?php

namespace Tinderbox\ClickhouseBuilder\Integrations\Laravel;

use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Traits\Macroable;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Query;
use Tinderbox\Clickhouse\Query\QueryStatistic;
use Tinderbox\ClickhouseBuilder\Query\BaseBuilder;
use Tinderbox\ClickhouseBuilder\Query\Expression;
use Tinderbox\ClickhouseBuilder\Query\Grammar;

class Builder extends BaseBuilder
{
    use Macroable {
        __call as macroCall;
    }

    /**
     * Connection which is used to perform queries.
     *
     * @var \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection
     */
    protected $connection;

    /**
     * Builder constructor.
     *
     * @param \Tinderbox\ClickhouseBuilder\Integrations\Laravel\Connection $connection
     */
    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->grammar = new Grammar();
    }

    /**
     * Perform compiled from builder sql query and getting result.
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return \Tinderbox\Clickhouse\Query\Result|\Tinderbox\Clickhouse\Query\Result[]
     */
    public function get()
    {
        if (!empty($this->async)) {
            return $this->connection->selectAsync($this->toAsyncQueries());
        } else {
            return $this->connection->select($this->toSql(), [], $this->getFiles());
        }
    }

    /**
     * Returns Query instance.
     *
     * @param array $settings
     *
     * @return Query
     */
    public function toQuery(array $settings = []): Query
    {
        return new Query($this->connection->getServer(), $this->toSql(), $this->getFiles(), $settings);
    }

    /**
     * Performs compiled sql for count rows only. May be used for pagination
     * Works only without async queries.
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return int|mixed
     */
    public function count()
    {
        $builder = $this->getCountQuery();
        $result = $builder->get();

        if (!empty($this->groups)) {
            return count($result);
        } else {
            return $result[0]['count'] ?? 0;
        }
    }

    /**
     * Perform query and get first row.
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return mixed|null|\Tinderbox\Clickhouse\Query\Result
     */
    public function first()
    {
        $result = $this->limit(1)->get();

        return $result[0] ?? null;
    }

    /**
     * Makes clean instance of builder.
     *
     * @return self
     */
    public function newQuery(): Builder
    {
        return new static($this->connection);
    }

    /**
     * Insert in table data from files.
     *
     * @param array  $columns
     * @param array  $files
     * @param string $format
     * @param int    $concurrency
     *
     * @return array
     */
    public function insertFiles(array $columns, array $files, string $format = Format::CSV, int $concurrency = 5): array
    {
        return $this->connection->insertFiles((string) $this->getFrom()->getTable(), $columns, $files, $format, $concurrency);
    }

    /**
     * Insert in table data from files.
     *
     * @param array                                                 $columns
     * @param string|\Tinderbox\Clickhouse\Interfaces\FileInterface $file
     * @param string                                                $format
     *
     * @return bool
     */
    public function insertFile(array $columns, $file, string $format = Format::CSV): bool
    {
        $file = $this->prepareFile($file);

        $result = $this->connection->insertFiles($this->getFrom()->getTable(), $columns, [$file], $format);

        return $result[0][0];
    }

    /**
     * Performs insert query.
     *
     * @param array $values
     * @param bool  $skipSort
     *
     * @throws \Tinderbox\ClickhouseBuilder\Exceptions\GrammarException
     *
     * @return bool
     */
    public function insert(array $values, bool $skipSort = false)
    {
        if (empty($values)) {
            return false;
        }

        if (!is_array(reset($values))) {
            $values = [$values];
        } /*
         * Here, we will sort the insert keys for every record so that each insert is
         * in the same order for the record. We need to make sure this is the case
         * so there are not any errors or problems when inserting these records.
         */
        elseif (!$skipSort) {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        return $this->connection->insert($this->grammar->compileInsert($this, $values));
    }

    /**
     * Performs ALTER TABLE `table` DELETE query.
     *
     * @throws \Tinderbox\ClickhouseBuilder\Exceptions\GrammarException
     *
     * @return int
     */
    public function delete()
    {
        return $this->connection->delete($this->grammar->compileDelete($this));
    }

    /**
     * Paginate the given query.
     *
     * @param int $page
     * @param int $perPage
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return LengthAwarePaginator
     */
    public function paginate(int $page = 1, int $perPage = 15): LengthAwarePaginator
    {
        $count = (int) $this->getConnection()
            ->table($this->cloneWithout(['columns' => [], 'orders' => [], 'limit' => null])
            ->select(new Expression('1')))
            ->count();

        $results = $this->limit($perPage, $perPage * ($page - 1))->get();

        return new LengthAwarePaginator(
            $results,
            $count,
            $perPage,
            $page
        );
    }

    /**
     * Get last query statistics from the connection.
     *
     * @throws \Tinderbox\ClickhouseBuilder\Exceptions\BuilderException
     *
     * @return QueryStatistic
     */
    public function getLastQueryStatistics(): QueryStatistic
    {
        return $this->getConnection()->getLastQueryStatistic();
    }

    /**
     * Get connection.
     *
     * @return Connection
     */
    public function getConnection(): Connection
    {
        return $this->connection;
    }
}
