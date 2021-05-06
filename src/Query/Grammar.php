<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Tinderbox\ClickhouseBuilder\Exceptions\GrammarException;
use Tinderbox\ClickhouseBuilder\Query\Enums\Format;
use Tinderbox\ClickhouseBuilder\Query\Traits\ArrayJoinComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\ColumnsComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\FormatComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\FromComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\GroupsComponentCompiler;
use Tinderbox\ClickhouseBuilder\Query\Traits\HavingsComponentCompiler;
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
    use ColumnsComponentCompiler;
    use FromComponentCompiler;
    use ArrayJoinComponentCompiler;
    use JoinComponentCompiler;
    use TwoElementsLogicExpressionsCompiler;
    use WheresComponentCompiler;
    use PreWheresComponentCompiler;
    use HavingsComponentCompiler;
    use SampleComponentCompiler;
    use GroupsComponentCompiler;
    use OrdersComponentCompiler;
    use LimitComponentCompiler;
    use LimitByComponentCompiler;
    use UnionsComponentCompiler;
    use FormatComponentCompiler;
    use TupleCompiler;

    protected $selectComponents = [
        'columns',
        'from',
        'sample',
        'arrayJoin',
        'joins',
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
    public function compileInsert(BaseBuilder $query, $values): string
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

        $format = $query->getFormat() ?? Format::VALUES;

        if ($format == Format::VALUES) {
            $columns = array_map(function ($col) {
                return is_string($col) ? new Identifier($col) : null;
            }, array_keys($values[0]));

            $columns = array_filter($columns);
        }

        $columns = $this->compileTuple(new Tuple($columns));

        $result[] = "INSERT INTO {$table}";

        if ($columns !== '') {
            $result[] = "({$columns})";
        }

        $result[] = 'FORMAT '.$format;

        if ($format == Format::VALUES) {
            $result[] = $this->compileInsertValues($values);
        }

        return implode(' ', $result);
    }

    /**
     * Compiles create table query.
     *
     * @param        $tableName
     * @param string $engine
     * @param array  $structure
     * @param bool   $ifNotExists
     *
     * @return string
     */
    public function compileCreateTable($tableName, string $engine, array $structure, $ifNotExists = false): string
    {
        if ($tableName instanceof Identifier) {
            $tableName = (string) $tableName;
        }

        return 'CREATE TABLE '.($ifNotExists ? 'IF NOT EXISTS ' : '')."{$tableName} ({$this->compileTableStructure($structure)}) ENGINE = {$engine}";
    }

    /**
     * Compiles drop table query.
     *
     * @param      $tableName
     * @param bool $ifExists
     *
     * @return string
     */
    public function compileDropTable($tableName, $ifExists = false): string
    {
        if ($tableName instanceof Identifier) {
            $tableName = (string) $tableName;
        }

        return 'DROP TABLE '.($ifExists ? 'IF EXISTS ' : '')."{$tableName}";
    }

    /**
     * Assembles table structure.
     *
     * @param array $structure
     *
     * @return string
     */
    public function compileTableStructure(array $structure): string
    {
        $result = [];

        foreach ($structure as $column => $type) {
            $result[] = $column.' '.$type;
        }

        return implode(', ', $result);
    }

    public function compileInsertValues($values)
    {
        return implode(', ', array_map(function ($value) {
            return '('.implode(', ', array_map(function ($value) {
                return $this->wrap($value);
            }, $value)).')';
        }, $values));
    }

    /**
     * Compile delete query.
     *
     * @param BaseBuilder $query
     *
     * @throws GrammarException
     *
     * @return string
     */
    public function compileDelete(BaseBuilder $query)
    {
        $this->verifyFrom($query->getFrom());

        $sql = "ALTER TABLE {$this->wrap($query->getFrom()->getTable())}";

        if (!is_null($query->getOnCluster())) {
            $sql .= " ON CLUSTER {$query->getOnCluster()}";
        }

        $sql .= ' DELETE';

        if (!is_null($query->getWheres()) && !empty($query->getWheres())) {
            $sql .= " {$this->compileWheresComponent($query, $query->getWheres())}";
        } else {
            throw GrammarException::missedWhereForDelete();
        }

        return $sql;
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
            $value = addslashes($value);

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
        } elseif (is_null($value)) {
            return 'null';
        } else {
            return;
        }
    }
}
