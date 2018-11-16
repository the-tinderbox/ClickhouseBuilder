<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Tinderbox\Clickhouse\Client;
use Tinderbox\Clickhouse\Common\Format;
use Tinderbox\Clickhouse\Common\TempTable;
use Tinderbox\ClickhouseBuilder\Exceptions\BuilderException;

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
     * @return \Tinderbox\Clickhouse\Query\Result|\Tinderbox\Clickhouse\Query\Result[]
     */
    public function get()
    {
        if (!empty($this->async)) {
            return $this->client->read($this->toAsyncSqls());
        } else {
            return $this->client->readOne($this->toSql(), [], $this->getFiles());
        }
    }
    
    /**
     * Performs compiled sql for count rows only. May be used for pagination
     * Works only without async queries.
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
     * Makes clean instance of builder.
     *
     * @return self
     */
    public function newQuery(): self
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
     * @return array
     */
    public function insertFiles(array $columns, array $files, string $format = Format::CSV, int $concurrency = 5): array
    {
        foreach ($files as $i => $file) {
            $files[$i] = $this->prepareFile($file);
        }
        
        return $this->client->writeFiles($this->getFrom()->getTable(), $columns, $files, $format, [], $concurrency);
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

        $result = $this->client->writeFiles($this->getFrom()->getTable(), $columns, [$file], $format);

        return $result[0][0];
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
        
        return $this->client->writeOne(
            $this->grammar->compileInsert($this, $values),
            array_flatten($values)
        );
    }
    
    /**
     * Performs ALTER TABLE `table` DELETE query.
     *
     * @return bool
     */
    public function delete()
    {
        return $this->client->writeOne(
            $this->grammar->compileDelete($this)
        );
    }

    /**
     * Executes query to create table
     *
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     *
     * @return bool
     */
    public function createTable($tableName, string $engine, array $structure)
    {
        return $this->client->writeOne($this->grammar->compileCreateTable($tableName, $engine, $structure));
    }

    /**
     * Executes query to create table if table does not exists
     *
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     *
     * @return bool
     */
    public function createTableIfNotExists($tableName, string $engine, array $structure)
    {
        return $this->client->writeOne($this->grammar->compileCreateTable($tableName, $engine, $structure, true));
    }

    public function dropTable($tableName)
    {
        return $this->client->writeOne($this->grammar->compileDropTable($tableName));
    }

    public function dropTableIfExists($tableName)
    {
        return $this->client->writeOne($this->grammar->compileDropTable($tableName, true));
    }
}
