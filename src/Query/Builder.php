<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Tinderbox\Clickhouse\Client;

class Builder extends BaseBuilder
{
    /**
     * Client which is used to perform queries.
     *
     * @var \Tinderbox\Clickhouse\Client
     */
    protected $client;

    /**
     * Builder constructor.
     *
     * @param Client $client
     */
    public function __construct(Client $client)
    {
        $this->client = $client;
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
            return $this->client->selectAsync($this->toAsyncSqls());
        } else {
            return $this->client->select($this->toSql());
        }
    }

    /**
     * Makes clean instance of builder.
     *
     * @return self
     */
    public function newQuery() : Builder
    {
        return new static($this->client);
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
    public function insertFiles(array $columns, array $files, string $format = \Tinderbox\Clickhouse\Common\Format::CSV, int $concurrency = 5) : array
    {
        return $this->client->insertFiles($this->getFrom()->getTable(), $columns, $files, $format, $concurrency);
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

        if (!is_array(reset($values))) {
            $values = [$values];
        }

        /*
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

        return $this->client->insert(
            $this->grammar->compileInsert($this, $values),
            array_flatten($values)
        );
    }
}
