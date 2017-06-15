<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Query\BaseBuilder;
use Tinderbox\ClickhouseBuilder\Query\Column;

trait ColumnsComponentCompiler
{
    use ColumnCompiler;

    /**
     * Compiles columns for select statement.
     *
     * @param BaseBuilder $builder
     * @param Column[]    $columns
     *
     * @return string
     */
    private function compileColumnsComponent(BaseBuilder $builder, array $columns) : string
    {
        $columns = array_reduce($columns, function ($columns, $column) {
            $columns[] = $this->compileColumn($column);

            return $columns;
        }, []);

        return implode(', ', $columns);
    }
}
