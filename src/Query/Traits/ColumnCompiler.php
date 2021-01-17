<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Query\Column;
use Tinderbox\ClickhouseBuilder\Query\Expression;

trait ColumnCompiler
{
    /**
     * Compiles column in string to pass this string in query.
     *
     * @param Column $column
     *
     * @return string
     */
    public function compileColumn(Column $column): string
    {
        $result = '';

        $columnName = $column->getColumnName();

        if ($columnName instanceof Column) {
            $columnName = new Expression("({$this->compileColumn($columnName)})");
        }

        foreach ($column->getFunctions() as $function) {
            $functionName = $function['function'];
            $params = $function['params'] ?? [];

            $result = $this->{$functionName}(empty($result) ? $this->wrap($columnName) : new Expression($result), $params);
        }

        if (empty($result) && !is_null($columnName)) {
            $result = $this->wrap($columnName);
        }

        if (!is_null($column->getAlias())) {
            $result .= " AS {$this->wrap($column->getAlias())}";
        }

        return $result;
    }

    /**
     * Compiles plus function on column.
     *
     * @param $firstValue
     * @param $secondValue
     *
     * @return string
     */
    private function plus($firstValue, $secondValue)
    {
        return "{$firstValue} + {$this->wrap($secondValue)}";
    }

    /**
     * Compiles multiple function on column.
     *
     * @param $firstValue
     * @param $secondValue
     *
     * @return string
     */
    private function multiple($firstValue, $secondValue)
    {
        return "{$firstValue} * {$this->wrap($secondValue)}";
    }

    /**
     * Compiles runningDifference function on column.
     *
     * @param $column
     *
     * @return string
     */
    private function runningDifference($column)
    {
        return "runningDifference({$column})";
    }

    private function count()
    {
        return 'count()';
    }

    /**
     * Compiles sumIf function on column.
     *
     * @param $column
     * @param $condition
     *
     * @return string
     */
    private function sumIf($column, $condition)
    {
        $condition = is_array($condition) ? implode(' ', $condition) : $condition;

        return "sumIf({$column}, {$condition})";
    }

    /**
     * Compiles sum function on column.
     *
     * @param $column
     *
     * @return string
     */
    private function sum($column)
    {
        return "sum({$column})";
    }

    /**
     * Compiles round function on column.
     *
     * @param $column
     * @param $decimals
     *
     * @return string
     */
    private function round($column, $decimals)
    {
        return "round({$column}, {$decimals})";
    }

    /**
     * Compiles distinct function on column.
     *
     * @param $column
     *
     * @return string
     */
    private function distinct($column)
    {
        return "DISTINCT {$column}";
    }
}
