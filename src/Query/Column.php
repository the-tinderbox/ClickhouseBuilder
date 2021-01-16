<?php

namespace Tinderbox\ClickhouseBuilder\Query;

class Column
{
    /**
     * Column name.
     *
     * @var Identifier|Expression|null
     */
    private $columnName;

    /**
     * Column alias.
     *
     * @var Identifier|null
     */
    private $alias;

    /**
     * Functions applied to column.
     *
     * Multidimensional array:
     * [['function' => 'functionName', 'params' => 'params'], []]
     *
     * Column name should NOT be passed in parameters
     *
     * @var array
     */
    private $functions = [];

    /**
     * Used for sub-queries.
     *
     * Stored here for ability to merge bindings from sub-query with query
     *
     * @var Builder
     */
    private $query;

    /**
     * Used for sub-queries, which executes not in callback.
     *
     * @var Builder|null
     */
    private $subQuery;

    /**
     * Column constructor.
     *
     * @param BaseBuilder $query
     */
    public function __construct(BaseBuilder $query)
    {
        $this->query = $query;
    }

    /**
     * Set column name.
     *
     * @param string|Expression $columnName
     *
     * @return Column
     */
    public function name($columnName): self
    {
        if ($columnName instanceof \Closure) {
            $columnName = tap(new static($this->query), $columnName);
        }

        if (is_string($columnName)) {
            $columnName = new Identifier($columnName);
        }

        $this->columnName = $columnName;

        return $this;
    }

    /**
     * Set alias for column.
     *
     * @param string $alias
     *
     * @return Column
     */
    public function as(string $alias): self
    {
        $this->alias = new Identifier($alias);

        return $this;
    }

    /**
     * Alias for as method.
     *
     * @param string $alias
     *
     * @return Column
     */
    public function alias(string $alias): self
    {
        return $this->as($alias);
    }

    /**
     * Converts expression to string.
     *
     * @param $expression
     *
     * @return string
     */
    private function expressionToString($expression)
    {
        if (is_array($expression)) {
            $expression = array_map(function ($element) {
                return (string) $element;
            }, $expression);

            return implode(' ', $expression);
        }

        return $expression;
    }

    /**
     * Get column name.
     *
     * @return Identifier|Expression|null
     */
    public function getColumnName()
    {
        return $this->columnName;
    }

    /**
     * Get column alias.
     *
     * @return Identifier|null
     */
    public function getAlias(): ?Identifier
    {
        return $this->alias;
    }

    /**
     * Get functions applied to column.
     *
     * @return array
     */
    public function getFunctions(): array
    {
        return $this->functions;
    }

    /**
     * Get sub-query.
     *
     * @return Builder|null
     */
    public function getSubQuery(): ?Builder
    {
        return $this->subQuery;
    }

    /**
     * Apply runningDIfference function to column.
     *
     * @return $this
     */
    public function runningDifference()
    {
        $this->functions[] = ['function' => 'runningDifference'];

        return $this;
    }

    /**
     * Apply sumIf function to column.
     *
     * @param array|mixed $expression
     *
     * @return $this
     */
    public function sumIf($expression = [])
    {
        $expression = is_array($expression) ? $expression : func_get_args();

        $expression = $this->expressionToString($expression);

        $this->functions[] = ['function' => 'sumIf', 'params' => $expression];

        return $this;
    }

    /**
     * Apply sum function to column.
     *
     * @param string|Expression|null $columnName
     *
     * @return $this
     */
    public function sum($columnName = null): self
    {
        if ($columnName !== null) {
            $this->name($columnName);
        }

        $this->functions[] = ['function' => 'sum'];

        return $this;
    }

    /**
     * Apply round function to column.
     *
     * @param int $decimals
     *
     * @return $this
     */
    public function round(int $decimals = 0): self
    {
        $this->functions[] = ['function' => 'round', 'params' => $decimals];

        return $this;
    }

    /**
     * Apply plus function to column.
     *
     * @param $value
     *
     * @return $this
     */
    public function plus($value)
    {
        $value = $this->expressionToString($value);

        $this->functions[] = ['function' => 'plus', 'params' => $value];

        return $this;
    }

    /**
     * Since that function doesn't take any arguments, then that function are collapse all other functions
     * which was called before. And as result will be only count() statement on place of this column.
     */
    public function count()
    {
        $this->functions[] = ['function' => 'count'];

        return $this;
    }

    /**
     * Apply distinct function to column.
     */
    public function distinct()
    {
        $this->functions[] = ['function' => 'distinct'];

        return $this;
    }

    /**
     * Apply multiple function to column.
     *
     * @param $value
     *
     * @return $this
     */
    public function multiple($value)
    {
        $value = $this->expressionToString($value);

        $this->functions[] = ['function' => 'multiple', 'params' => $value];

        return $this;
    }

    /**
     * Return sub-query.
     *
     * @return Builder
     */
    public function subQuery(): Builder
    {
        return $this->subQuery = $this->query->newQuery();
    }

    /**
     * Execute sub-query in select statement of column.
     *
     * @param \Closure|Builder|null $query
     *
     * @return Column|Builder
     */
    public function query($query = null)
    {
        if (is_null($query)) {
            return $this->subQuery();
        }

        if ($query instanceof \Closure) {
            $query = tap($this->query->newQuery(), $query);
        }

        if ($query instanceof BaseBuilder) {
            if (is_null($this->alias) && !is_null($this->columnName)) {
                $this->alias($this->columnName);
            }

            $this->name(new Expression("({$query->toSql()})"));
        }

        return $this;
    }
}
