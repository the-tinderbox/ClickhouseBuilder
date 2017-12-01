<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use function MongoDB\is_string_array;
use Tinderbox\ClickhouseBuilder\Exceptions\GrammarException;
use Tinderbox\ClickhouseBuilder\Query\Enums\Format;
use Tinderbox\ClickhouseBuilder\Query\Traits\ColumnsComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\FormatComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\FromComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\GroupsComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\HavingsComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\ArrayJoinComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\JoinComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\LimitByComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\LimitComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\OrdersComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\PreWheresComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\SampleComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\TupleCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\TwoElementsLogicExpressionsCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\UnionsComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\WheresComponentCompiler;

class Grammar
{
    use ColumnsComponentCompiler,
        FromComponentCompiler,
        ArrayJoinComponentCompiler,
        JoinComponentCompiler,
        TwoElementsLogicExpressionsCompiler,
        WheresComponentCompiler,
        PreWheresComponentCompiler,
        HavingsComponentCompiler,
        SampleComponentCompiler,
        GroupsComponentCompiler,
        OrdersComponentCompiler,
        LimitComponentCompiler,
        LimitByComponentCompiler,
        UnionsComponentCompiler,
        FormatComponentCompiler,
        TupleCompiler;

    protected $selectComponents = [
        'columns',
        'from',
        'sample',
        'arrayJoin',
        'join',
        'prewheres',
        'wheres',
        'groups',
        'havings',
        'orders',
        'limitBy',
        'limit',
        'unions',
        'format',
    ];

    /**
     * Compiles select query.
     *
     * @param BaseBuilder $query
     *
     * @return string
     */
    public function compileSelect(BaseBuilder $query)
    {
        if (empty($query->getColumns())) {
            $query->select();
        }

        $sql = [];

        foreach ($this->selectComponents as $component) {
            $compileMethod = 'compile'.ucfirst($component).'Component';
            $component = 'get'.ucfirst($component);

            if (!is_null($query->$component()) && !empty($query->$component())) {
                $sql[$component] = $this->$compileMethod($query, $query->$component());
            }
        }

        return trim('SELECT '.trim(implode(' ', $sql)));
    }

    /**
     * Compile insert query for values.
     *
     * @param BaseBuilder $query
     * @param             $values
     *
     * @throws GrammarException
     *
     * @return string
     */
    public function compileInsert(BaseBuilder $query, $values) : string
    {
        $result = [];

        $from = $query->getFrom();

        if (is_null($from)) {
            throw GrammarException::missedTableForInsert();
        }

        $table = $this->wrap($from->getTable());

        if (is_null($table)) {
            throw GrammarException::missedTableForInsert();
        }

        $columns = array_map(function ($col) {
            return is_string($col) ? new Identifier($col) : null;
        }, array_keys($values[0]));

        $columns = array_filter($columns);

        $columns = $this->compileTuple(new Tuple($columns));

        $result[] = "INSERT INTO {$table}";

        if ($columns !== '') {
            $result[] = "({$columns})";
        }

        $result[] = 'FORMAT '.($query->getFormat() ?? Format::VALUES);

        // glue together all the values
        $result[] = implode(', ', array_map(function (array $columns) {
            $placeholders = array_map(function ($columnValue) {
                // if inserted value is Array
                if (is_array($columnValue)) {
                    return sprintf(
                        '[%s]',
                        implode(', ', array_fill(0, count($columnValue), '?'))
                    );
                }
                return '?';
            }, $columns);

            return sprintf('(%s)', implode(', ', $placeholders));
        }, $values));

        return implode(' ', $result);
    }

    /**
     * Convert value in literal.
     *
     * @param string|Expression|Identifier|array $value
     *
     * @return string|array|null|int
     */
    public function wrap($value)
    {
        if ($value instanceof Expression) {
            return $value->getValue();
        } elseif (is_array($value)) {
            return array_map([$this, 'wrap'], $value);
        } elseif (is_string($value)) {
            return "'{$value}'";
        } elseif ($value instanceof Identifier) {
            $value = (string) $value;

            if (strpos(strtolower($value), '.') !== false) {
                return implode('.', array_map(function ($element) {
                    return $this->wrap(new Identifier($element));
                }, array_map('trim', preg_split('/\./', $value))));
            }

            if (strpos(strtolower($value), ' as ') !== false) {
                list($value, $alias) = array_map('trim', preg_split('/\s+as\s+/i', $value));

                $value = $this->wrap(new Identifier($value));
                $alias = $this->wrap(new Identifier($alias));

                $value = "$value AS $alias";

                return $value;
            }

            if ($value === '*') {
                return $value;
            }

            return '`'.str_replace('`', '``', $value).'`';
        } elseif (is_numeric($value)) {
            return $value;
        } else {
            return;
        }
    }
}
