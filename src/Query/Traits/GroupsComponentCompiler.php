<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Query\BaseBuilder as Builder;
use Tinderbox\ClickhouseBuilder\Query\Column;

trait GroupsComponentCompiler
{
    /**
     * Compiles groupings to string to pass this string in query
     *
     * @param Builder $builder
     * @param Column[] $columns
     *
     * @return string
     */
    private function compileGroupsComponent(Builder $builder, array $columns) : string
    {
        $columns = array_reduce($columns, function ($columns, $column) {

            $columns[] = $this->compileColumn($column);

            return $columns;
        }, []);

        if (!empty($columns) && !in_array('*', $columns, true)) {
            return 'GROUP BY '.implode(', ', $columns);
        } else {
            return '';
        }
    }
}