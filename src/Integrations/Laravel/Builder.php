<?php

namespace Tinderbox\ClickhouseBuilder\Integrations\Laravel;

use Illuminate\Support\Traits\Macroable;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Query;
use Tinderbox\ClickhouseBuilder\Query\BaseBuilder;
use Tinderbox\ClickhouseBuilder\Query\Grammar;
use Illuminate\Pagination\Paginator;
use Illuminate\Container\Container;
use Illuminate\Pagination\LengthAwarePaginator;

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
        if (! empty($this->async)) {
            return $this->connection->selectAsync($this->toAsyncQueries());
        } else {
            return $this->connection->select($this->toSql(), [], $this->getFiles());
        }
    }
    
    /**
     * Returns Query instance
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
     * @param string $column Column to pass into count() aggregate function
     *
     * @return int|mixed
     */
    public function count()
    {
        $builder = $this->getCountQuery();
        $result = $builder->get();

        if (! empty($this->groups)) {
            return count($result);
        } else {
            return $result[0]['count'] ?? 0;
        }
    }

    /**
     * Paginate the given query.
     *
     * @param  int  $perPage
     * @param  array  $columns
     * @param  string  $pageName
     * @param  int|null  $page
     * @return \Illuminate\Contracts\Pagination\LengthAwarePaginator
     *
     * @throws \InvalidArgumentException
     */
    public function paginate($perPage = null, $columns = ['*'], $pageName = 'page', $page = null)
    {
        $page = $page ?: Paginator::resolveCurrentPage($pageName);
        $perPage = $perPage ?: 30;

        $results = ($total = $this->count())
            ? $this->forPage($page, $perPage)->get($columns)
            : $this->model->newCollection();

        return $this->paginator($results, $total, $perPage, $page, [
            'path' => Paginator::resolveCurrentPath(),
            'pageName' => $pageName,
        ]);
    }
    /**
     * Create a new length-aware paginator instance.
     *
     * @param  \Illuminate\Support\Collection  $items
     * @param  int  $total
     * @param  int  $perPage
     * @param  int  $currentPage
     * @param  array  $options
     * @return \Illuminate\Pagination\LengthAwarePaginator
     */
    protected function paginator($items, $total, $perPage, $currentPage, $options)
    {
        return Container::getInstance()->makeWith(LengthAwarePaginator::class, compact(
            'items', 'total', 'perPage', 'currentPage', 'options'
        ));
    }

    /**
     * Set the limit and offset for a given page.
     *
     * @param  int  $page
     * @param  int  $perPage
     * @return \Illuminate\Database\Query\Builder|static
     */
    public function forPage($page, $perPage = 15)
    {
        return $this->limit($perPage, ($page - 1) * $perPage);
    }

    /**
     * Perform query and get first row
     *
     * @return mixed|null|\Tinderbox\Clickhouse\Query\Result
     */
    public function first()
    {
        $result = $this->get();

        return $result[0] ?? null;
    }

    /**
     * Makes clean instance of builder.
     *
     * @return self
     */
    public function newQuery() : Builder
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
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return array
     */
    public function insertFiles(array $columns, array $files, string $format = Format::CSV, int $concurrency = 5) : array
    {
        return $this->connection->insertFiles((string)$this->getFrom()->getTable(), $columns, $files, $format, $concurrency);
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
     * Insert in table data from files.
     *
     * @param array  $columns
     * @param array  $files
     * @param string $format
     *
     * @throws \Tinderbox\Clickhouse\Exceptions\ClientException
     *
     * @return array
     */
    public function insertFilesAsOne(array $columns, array $files, string $format = Format::CSV) : array
    {
        return $this->connection->insertFilesAsOne((string) $this->getFrom()->getTable(), $columns, $files, $format);
    }

    /**
     * Performs insert query.
     *
     * @param array $values
     *
     * @return bool
     */
    public function insert(array $values)
    {
        if (empty($values)) {
            return false;
        }

        if (! is_array(reset($values))) {
            $values = [$values];
        } /*
         * Here, we will sort the insert keys for every record so that each insert is
         * in the same order for the record. We need to make sure this is the case
         * so there are not any errors or problems when inserting these records.
         */
        else {
            foreach ($values as $key => $value) {
                ksort($value);
                $values[$key] = $value;
            }
        }

        return $this->connection->insert(
            $this->grammar->compileInsert($this, $values),
            array_flatten($values)
        );
    }

    /**
     * Performs ALTER TABLE `table` DELETE query.
     *
     * @return int
     */
    public function delete()
    {
        return $this->connection->delete($this->grammar->compileDelete($this));
    }
}
