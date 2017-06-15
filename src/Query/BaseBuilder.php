<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Closure;
use Tinderbox\ClickhouseBuilder\Query\Enums\Format;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinStrict;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinType;
use Tinderbox\ClickhouseBuilder\Query\Enums\Operator;
use Tinderbox\ClickhouseBuilder\Query\Enums\OrderDirection;

abstract class BaseBuilder
{
    /**
     * Columns for select.
     *
     * @var Column[]
     */
    protected $columns = [];

    /**
     * Table to select from.
     *
     * @var From|null
     */
    protected $from = null;

    /**
     * Sample expression.
     *
     * @var float|null
     */
    protected $sample;

    /**
     * Join clause.
     *
     * @var JoinClause
     */
    protected $join;

    /**
     * Prewhere statements.
     *
     * @var TwoElementsLogicExpression[]
     */
    protected $prewheres = [];

    /**
     * Where statements.
     *
     * @var TwoElementsLogicExpression[]
     */
    protected $wheres = [];

    /**
     * Groupings.
     *
     * @var array
     */
    protected $groups = [];

    /**
     * Having statements.
     *
     * @var TwoElementsLogicExpression[]
     */
    protected $havings = [];

    /**
     * Order statements.
     *
     * @var array
     */
    protected $orders = [];

    /**
     * Limit.
     *
     * @var Limit|null
     */
    protected $limit;

    /**
     * Limit n by statement.
     *
     * @var Limit|null
     */
    protected $limitBy;

    /**
     * Queries to union.
     *
     * @var array
     */
    protected $unions = [];

    /**
     * Query format.
     *
     * @var Format|null
     */
    protected $format;

    /**
     * Grammar to build query parts.
     *
     * @var Grammar
     */
    protected $grammar;

    /**
     * Queries which must be run asynchronous.
     *
     * @var array
     */
    protected $async = [];

    /**
     * Set columns for select statement.
     *
     * @param array|mixed $columns
     *
     * @return self
     */
    public function select(...$columns) : self
    {
        $columns = isset($columns[0]) && is_array($columns[0]) ? $columns[0] : $columns;

        if (empty($columns)) {
            $columns[] = '*';
        }

        $this->columns = $this->processColumns($columns);

        return $this;
    }

    /**
     * Add columns to exist select statement.
     *
     * @param array|mixed $columns
     *
     * @return self
     */
    public function addSelect(...$columns) : self
    {
        $columns = isset($columns[0]) && is_array($columns[0]) ? $columns[0] : $columns;

        if (empty($columns)) {
            $columns[] = '*';
        }

        $this->columns = array_merge($this->columns, $this->processColumns($columns));

        return $this;
    }

    /**
     * Prepares columns given by user to Column objects.
     *
     * @param array $columns
     * @param bool  $withAliases
     *
     * @return array
     */
    protected function processColumns(array $columns, bool $withAliases = true) : array
    {
        $result = [];

        foreach ($columns as $column => $value) {
            if ($value instanceof Closure) {
                $columnName = $column;
                $column = (new Column($this));

                if (!is_int($columnName)) {
                    $column->name($columnName);
                }

                $column = tap($column, $value);

                if ($column->getSubQuery()) {
                    $column->query($column->getSubQuery());
                }
            }

            if ($value instanceof self) {
                $alias = is_string($column) ? $column : null;
                $column = (new Column($this))->query($value);

                if (!is_null($alias) && $withAliases) {
                    $column->as($alias);
                }
            }

            if (is_int($column)) {
                $column = $value;
                $value = null;
            }

            if (!$column instanceof Column) {
                $alias = is_string($value) ? $value : null;

                $column = (new Column($this))->name($column);

                if (!is_null($alias) && $withAliases) {
                    $column->as($alias);
                }
            }

            $result[] = $column;
        }

        return $result;
    }

    /**
     * Sets table to from statement.
     *
     * @param Closure|Builder|string $table
     * @param string                 $alias
     * @param bool                   $isFinal
     *
     * @return self
     */
    public function from($table, string $alias = null, bool $isFinal = null) : self
    {
        $this->from = new From($this);

        /*
         * If builder instance given, then we assume that from section should contain sub-query
         */
        if ($table instanceof self) {
            $this->from->query($table);
        }

        /*
         * If closure given, then we call it and pass From object as argument to
         * set up From object in callback
         */
        if ($table instanceof Closure) {
            $table($this->from);
        }

        /*
         * If given anything that is not builder instance or callback. For example, string,
         * then we assume that table name was given.
         */
        if (!$table instanceof Closure && !$table instanceof self) {
            $this->from->table($table);
        }

        if (!is_null($alias)) {
            $this->from->as($alias);
        }

        if (!is_null($isFinal)) {
            $this->from->final($isFinal);
        }

        /*
         * If subQuery method was executed on From object, then we take subQuery and "execute" it
         */
        if (!is_null($this->from->getSubQuery())) {
            $this->from->query($this->from->getSubQuery());
        }

        return $this;
    }

    /**
     * Alias for from method.
     *
     * @param             $table
     * @param string|null $alias
     * @param bool|null   $isFinal
     *
     * @return Builder
     */
    public function table($table, string $alias = null, bool $isFinal = null) : self
    {
        return $this->from($table, $alias, $isFinal);
    }

    /**
     * Set sample expression.
     *
     * @param float $coefficient
     *
     * @return self
     */
    public function sample(float $coefficient) : self
    {
        $this->sample = $coefficient;

        return $this;
    }

    /**
     * Add queries to union with.
     *
     * @param self|Closure $query
     *
     * @return self
     */
    public function unionAll($query) : self
    {
        if ($query instanceof Closure) {
            $query = tap($this->newQuery(), $query);
        }

        if ($query instanceof self) {
            $this->unions[] = $query;
        } else {
            throw new \InvalidArgumentException('Argument for unionAll must be closure or builder instance.');
        }

        return $this;
    }

    /**
     * Set alias for table in from statement.
     *
     * @param string $alias
     *
     * @return self
     */
    public function as(string $alias) : self
    {
        $this->from->as($alias);

        return $this;
    }

    /**
     * As method alias.
     *
     * @param string $alias
     *
     * @return self
     */
    public function alias(string $alias) : self
    {
        return $this->as($alias);
    }

    /**
     * Sets final option on from statement.
     *
     * @param bool $final
     *
     * @return self
     */
    public function final(bool $final = true) : self
    {
        $this->from->final($final);

        return $this;
    }

    /**
     * Add join to query.
     *
     * @param string|self|Closure $table  Table to select from, also may be a sub-query
     * @param string|null         $strict All or any
     * @param string|null         $type   Left or inner
     * @param array|null          $using  Columns to use for join
     * @param bool                $global Global distribution for right table
     *
     * @return self
     */
    public function join(
        $table,
        string $strict = null,
        string $type = null,
        array $using = null,
        bool $global = false
    ) : self {
        $this->join = new JoinClause($this);

        /*
         * If builder instance given, then we assume that sub-query should be used as table in join
         */
        if ($table instanceof self) {
            $this->join->query($table);
        }

        /*
         * If closure given, then we call it and pass From object as argument to
         * set up JoinClause object in callback
         */
        if ($table instanceof Closure) {
            $table($this->join);
        }

        /*
         * If given anything that is not builder instance or callback. For example, string,
         * then we assume that table name was given.
         */
        if (!$table instanceof Closure && !$table instanceof self) {
            $this->join->table($table);
        }

        /*
         * If using was given, then merge it with using given before, in closure
         */
        if (!is_null($using)) {
            $this->join->addUsing($using);
        }

        if (!is_null($strict) && is_null($this->join->getStrict())) {
            $this->join->strict($strict);
        }

        if (!is_null($type) && is_null($this->join->getType())) {
            $this->join->type($type);
        }

        $this->join->distributed($global);

        if (!is_null($this->join->getSubQuery())) {
            $this->join->query($this->join->getSubQuery());
        }

        return $this;
    }

    /**
     * Left join.
     *
     * Alias for join method, but without specified strictness
     *
     * @param string|self|Closure $table
     * @param string|null         $strict
     * @param array|null          $using
     * @param bool                $global
     *
     * @return self
     */
    public function leftJoin($table, string $strict = null, array $using = null, bool $global = false) : self
    {
        return $this->join($table, $strict ?? JoinStrict::ALL, JoinType::LEFT, $using, $global);
    }

    /**
     * Inner join.
     *
     * Alias for join method, but without specified strictness
     *
     * @param string|self|Closure $table
     * @param string|null         $strict
     * @param array|null          $using
     * @param bool                $global
     *
     * @return self
     */
    public function innerJoin($table, string $strict = null, array $using = null, bool $global = false) : self
    {
        return $this->join($table, $strict ?? JoinStrict::ALL, JoinType::INNER, $using, $global);
    }

    /**
     * Any left join.
     *
     * Alias for join method, but with specified any strictness
     *
     * @param string|self|Closure $table
     * @param array|null          $using
     * @param bool                $global
     *
     * @return self
     */
    public function anyLeftJoin($table, array $using = null, bool $global = false) : self
    {
        return $this->join($table, JoinStrict::ANY, JoinType::LEFT, $using, $global);
    }

    /**
     * All left join.
     *
     * Alias for join method, but with specified all strictness.
     *
     * @param string|self|Closure $table
     * @param array|null          $using
     * @param bool                $global
     *
     * @return self
     */
    public function allLeftJoin($table, array $using = null, bool $global = false) : self
    {
        return $this->join($table, JoinStrict::ALL, JoinType::LEFT, $using, $global);
    }

    /**
     * Any inner join.
     *
     * Alias for join method, but with specified any strictness.
     *
     * @param string|self|Closure $table
     * @param array|null          $using
     * @param bool                $global
     *
     * @return self
     */
    public function anyInnerJoin($table, array $using = null, bool $global = false) : self
    {
        return $this->join($table, JoinStrict::ANY, JoinType::INNER, $using, $global);
    }

    /**
     * All inner join.
     *
     * Alias for join method, but with specified all strictness.
     *
     * @param string|self|Closure $table
     * @param array|null          $using
     * @param bool                $global
     *
     * @return self
     */
    public function allInnerJoin($table, array $using = null, bool $global = false) : self
    {
        return $this->join($table, JoinStrict::ALL, JoinType::INNER, $using, $global);
    }

    /**
     * Get two elements logic expression to put it in the right place.
     *
     *
     * Used in where, prewhere and having methods.
     *
     * @param TwoElementsLogicExpression|string|Closure|self $column
     * @param mixed                                          $operator
     * @param mixed                                          $value
     * @param string                                         $concatOperator
     * @param string                                         $section
     *
     * @return TwoElementsLogicExpression
     */
    protected function assembleTwoElementsLogicExpression(
        $column,
        $operator,
        $value,
        string $concatOperator,
        string $section
    ) : TwoElementsLogicExpression {
        $expression = new TwoElementsLogicExpression($this);

        /*
         * If user passed TwoElementsLogicExpression as first argument, then we assume that user has set up himself.
         */
        if ($column instanceof TwoElementsLogicExpression && is_null($value)) {
            return $column;
        }

        if ($column instanceof TwoElementsLogicExpression && $value instanceof TwoElementsLogicExpression) {
            $expression->firstElement($column);
            $expression->secondElement($value);
            $expression->operator($operator);
            $expression->concatOperator($concatOperator);

            return $expression;
        }

        /*
         * If closure, then we pass fresh query builder inside and based on their state after evaluating try to assume
         * what user expects to perform.
         * If resulting query builder have elements corresponding to requested section, then we assume that user wanted
         * to just wrap this in parenthesis, otherwise - subquery.
         */
        if ($column instanceof Closure) {
            $query = tap($this->newQuery(), $column);

            if (is_null($query->getFrom()) && empty($query->getColumns())) {
                $expression->firstElement($query->{"get{$section}"}());
            } else {
                $expression->firstElement(new Expression("({$query->toSql()})"));
            }
        }

        /*
         * If as column was passed builder instance, than we perform subquery in first element position.
         */
        if ($column instanceof self) {
            $expression->firstElementQuery($column);
        }

        /*
         * If builder instance given as value, then we assume that sub-query should be used there.
         */
        if ($value instanceof self || $value instanceof Closure) {
            $expression->secondElementQuery($value);
        }

        /*
         * Set up other parameters if none of them was set up before in TwoElementsLogicExpression object
         */
        if (is_null($expression->getFirstElement()) && !is_null($column)) {
            $expression->firstElement(is_string($column) ? new Identifier($column) : $column);
        }

        if (is_null($expression->getSecondElement()) && !is_null($value)) {
            if (is_array($value) && count($value) === 2 && Operator::isValid($operator) && in_array(
                    $operator,
                    [Operator::BETWEEN, Operator::NOT_BETWEEN]
                )
            ) {
                $value = (new TwoElementsLogicExpression($this))
                    ->firstElement($value[0])
                    ->operator(Operator::AND)
                    ->secondElement($value[1])
                    ->concatOperator($concatOperator);
            }

            if (is_array($value) && Operator::isValid($operator) && in_array(
                    $operator,
                    [Operator::IN, Operator::NOT_IN]
                )
            ) {
                $value = new Tuple($value);
            }

            $expression->secondElement($value);
        }

        $expression->concatOperator($concatOperator);

        if (is_string($operator)) {
            $expression->operator($operator);
        }

        return $expression;
    }

    /**
     * Prepare operator for where and prewhere statement.
     *
     * @param mixed  $value
     * @param string $operator
     * @param bool   $useDefault
     *
     * @return array
     */
    protected function prepareValueAndOperator($value, $operator, $useDefault = false) : array
    {
        if ($useDefault) {
            $value = $operator;

            if (is_array($value)) {
                $operator = Operator::IN;
            } else {
                $operator = Operator::EQUALS;
            }

            return [$value, $operator];
        }

        return [$value, $operator];
    }

    /**
     * Add prewhere statement.
     *
     * @param TwoElementsLogicExpression|self|Closure|string      $column
     * @param mixed                                               $operator
     * @param TwoElementsLogicExpression|self|Closure|string|null $value
     * @param string                                              $concatOperator
     *
     * @return self
     */
    public function preWhere($column, $operator = null, $value = null, string $concatOperator = Operator::AND) : self
    {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 2);

        $this->prewheres[] = $this->assembleTwoElementsLogicExpression(
            $column,
            $operator,
            $value,
            $concatOperator,
            'prewheres'
        );

        return $this;
    }

    /**
     * Add prewhere statement "as is".
     *
     * @param string $expression
     *
     * @return self
     */
    public function preWhereRaw(string $expression) : self
    {
        return $this->preWhere(new Expression($expression));
    }

    /**
     * Add prewhere statement "as is", but with OR operator.
     *
     * @param string $expression
     *
     * @return self
     */
    public function orPreWhereRaw(string $expression) : self
    {
        return $this->preWhere(new Expression($expression), null, null, Operator::OR);
    }

    /**
     * Add prewhere statement but with OR operator.
     *
     * @param      $column
     * @param null $operator
     * @param null $value
     *
     * @return self
     */
    public function orPreWhere($column, $operator = null, $value = null) : self
    {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 2);

        return $this->prewhere($column, $operator, $value, Operator::OR);
    }

    /**
     * Add prewhere statement with IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function preWhereIn($column, $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_IN : Operator::IN;

        if (is_array($values)) {
            $values = new Tuple($values);
        }

        return $this->preWhere($column, $type, $values, $boolean);
    }

    /**
     * Add prewhere statement with IN operator and OR operator.
     *
     * @param $column
     * @param $values
     *
     * @return self
     */
    public function orPreWhereIn($column, $values) : self
    {
        return $this->preWhereIn($column, $values, Operator::OR);
    }

    /**
     * Add prewhere statement with NOT IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function preWhereNotIn($column, $values, $boolean = Operator::AND) : self
    {
        return $this->preWhereIn($column, $values, $boolean, true);
    }

    /**
     * Add prewhere statement with NOT IN operator and OR operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function orPreWhereNotIn($column, $values, $boolean = Operator::OR) : self
    {
        return $this->preWhereNotIn($column, $values, $boolean);
    }

    /**
     * Add prewhere statement with BETWEEN simulation.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function preWhereBetween($column, array $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->preWhere($column, $type, [$values[0], $values[1]], $boolean);
    }

    /**
     * Add prewhere statement with BETWEEN simulation, but with column names as value.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function preWhereBetweenColumns($column, array $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->preWhere($column, $type, [new Identifier($values[0]), new Identifier($values[1])], $boolean);
    }

    /**
     * Add prewhere statement with NOT BETWEEN simulation, but with column names as value.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     *
     * @return self
     */
    public function preWhereNotBetweenColumns($column, array $values, $boolean = Operator::AND) : self
    {
        return $this->preWhere($column, Operator::NOT_BETWEEN, [new Identifier($values[0]), new Identifier($values[1])], $boolean);
    }

    /**
     * Add prewhere statement with BETWEEN simulation, but with column names as value and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orPreWhereBetweenColumns($column, array $values) : self
    {
        return $this->preWhereBetweenColumns($column, $values, Operator::OR);
    }

    /**
     * Add prewhere statement with NOT BETWEEN simulation, but with column names as value and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orPreWhereNotBetweenColumns($column, array $values) : self
    {
        return $this->preWhereNotBetweenColumns($column, $values, Operator::OR);
    }

    /**
     * Add prewhere statement with BETWEEN simulation and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orPreWhereBetween($column, array $values) : self
    {
        return $this->preWhereBetween($column, $values, Operator::OR);
    }

    /**
     * Add prewhere statement with NOT BETWEEN simulation.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     *
     * @return self
     */
    public function preWhereNotBetween($column, array $values, $boolean = Operator::AND) : self
    {
        return $this->preWhereBetween($column, $values, $boolean, true);
    }

    /**
     * Add prewhere statement with NOT BETWEEN simulation and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orPreWhereNotBetween($column, array $values) : self
    {
        return $this->preWhereNotBetween($column, $values, Operator::OR);
    }

    /**
     * Add where statement.
     *
     * @param TwoElementsLogicExpression|string|Closure|self $column
     * @param mixed                                          $operator
     * @param mixed                                          $value
     * @param string                                         $concatOperator
     *
     * @return self
     */
    public function where($column, $operator = null, $value = null, string $concatOperator = Operator::AND) : self
    {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 2);

        $this->wheres[] = $this->assembleTwoElementsLogicExpression(
            $column,
            $operator,
            $value,
            $concatOperator,
            'wheres'
        );

        return $this;
    }

    /**
     * Add where statement "as is".
     *
     * @param string $expression
     *
     * @return self
     */
    public function whereRaw(string $expression) : self
    {
        return $this->where(new Expression($expression));
    }

    /**
     * Add where statement "as is" with OR operator.
     *
     * @param string $expression
     *
     * @return self
     */
    public function orWhereRaw(string $expression) : self
    {
        return $this->where(new Expression($expression), null, null, Operator::OR);
    }

    /**
     * Add where statement with OR operator.
     *
     * @param      $column
     * @param null $operator
     * @param null $value
     *
     * @return self
     */
    public function orWhere($column, $operator = null, $value = null) : self
    {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 2);

        return $this->where($column, $operator, $value, Operator::OR);
    }

    /**
     * Add where statement with IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function whereIn($column, $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_IN : Operator::IN;

        if (is_array($values)) {
            $values = new Tuple($values);
        }

        return $this->where($column, $type, $values, $boolean);
    }

    /**
     * Add where statement with GLOBAL option and IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function whereGlobalIn($column, $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::GLOBAL_NOT_IN : Operator::GLOBAL_IN;

        if (is_array($values)) {
            $values = new Tuple($values);
        }

        return $this->where($column, $type, $values, $boolean);
    }

    /**
     * Add where statement with GLOBAL option and IN operator and OR operator.
     *
     * @param $column
     * @param $values
     *
     * @return self
     */
    public function orWhereGlobalIn($column, $values) : self
    {
        return $this->whereGlobalIn($column, $values, Operator::OR);
    }

    /**
     * Add where statement with GLOBAL option and NOT IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function whereGlobalNotIn($column, $values, $boolean = Operator::AND) : self
    {
        return $this->whereGlobalIn($column, $values, $boolean, true);
    }

    /**
     * Add where statement with GLOBAL option and NOT IN operator and OR operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function orWhereGlobalNotIn($column, $values, $boolean = Operator::OR) : self
    {
        return $this->whereGlobalNotIn($column, $values, $boolean);
    }

    /**
     * Add where statement with IN operator and OR operator.
     *
     * @param $column
     * @param $values
     *
     * @return self
     */
    public function orWhereIn($column, $values) : self
    {
        return $this->whereIn($column, $values, Operator::OR);
    }

    /**
     * Add where statement with NOT IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function whereNotIn($column, $values, $boolean = Operator::AND) : self
    {
        return $this->whereIn($column, $values, $boolean, true);
    }

    /**
     * Add where statement with NOT IN operator and OR operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function orWhereNotIn($column, $values, $boolean = Operator::OR) : self
    {
        return $this->whereNotIn($column, $values, $boolean);
    }

    /**
     * Add where statement with BETWEEN simulation.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function whereBetween($column, array $values, $boolean = Operator::AND, $not = false) : self
    {
        $operator = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->where($column, $operator, [$values[0], $values[1]], $boolean);
    }

    /**
     * Add where statement with BETWEEN simulation, but with column names as value.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function whereBetweenColumns($column, array $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->where($column, $type, [new Identifier($values[0]), new Identifier($values[1])], $boolean);
    }

    /**
     * Add where statement with BETWEEN simulation, but with column names as value and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orWhereBetweenColumns($column, array $values) : self
    {
        return $this->whereBetweenColumns($column, $values, Operator::OR);
    }

    /**
     * Add where statement with BETWEEN simulation and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orWhereBetween($column, array $values) : self
    {
        return $this->whereBetween($column, $values, Operator::OR);
    }

    /**
     * Add where statement with NOT BETWEEN simulation.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     *
     * @return self
     */
    public function whereNotBetween($column, array $values, $boolean = Operator::AND) : self
    {
        return $this->whereBetween($column, $values, $boolean, true);
    }

    /**
     * Add prewhere statement with NOT BETWEEN simulation and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orWhereNotBetween($column, array $values) : self
    {
        return $this->whereNotBetween($column, $values, Operator::OR);
    }

    /**
     * Add having statement.
     *
     * @param TwoElementsLogicExpression|string|Closure|self $column
     * @param mixed                                          $operator
     * @param mixed                                          $value
     * @param string                                         $concatOperator
     *
     * @return self
     */
    public function having($column, $operator = null, $value = null, string $concatOperator = Operator::AND) : self
    {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 2);

        $this->havings[] = $this->assembleTwoElementsLogicExpression($column, $operator, $value, $concatOperator, 'havings');

        return $this;
    }

    /**
     * Add having statement "as is".
     *
     * @param string $expression
     *
     * @return self
     */
    public function havingRaw(string $expression) : self
    {
        return $this->having(new Expression($expression));
    }

    /**
     * Add having statement "as is" with OR operator.
     *
     * @param string $expression
     *
     * @return self
     */
    public function orHavingRaw(string $expression) : self
    {
        return $this->having(new Expression($expression), null, null, Operator::OR);
    }

    /**
     * Add having statement with OR operator.
     *
     * @param      $column
     * @param null $operator
     * @param null $value
     *
     * @return self
     */
    public function orHaving($column, $operator = null, $value = null) : self
    {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 2);

        return $this->having($column, $operator, $value, Operator::OR);
    }

    /**
     * Add having statement with IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function havingIn($column, $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_IN : Operator::IN;

        if (is_array($values)) {
            $values = new Tuple($values);
        }

        return $this->having($column, $type, $values, $boolean);
    }

    /**
     * Add having statement with IN operator and OR operator.
     *
     * @param $column
     * @param $values
     *
     * @return self
     */
    public function orHavingIn($column, $values) : self
    {
        return $this->havingIn($column, $values, Operator::OR);
    }

    /**
     * Add having statement with NOT IN operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function havingNotIn($column, $values, $boolean = Operator::AND) : self
    {
        return $this->havingIn($column, $values, $boolean, true);
    }

    /**
     * Add having statement with NOT IN operator and OR operator.
     *
     * @param        $column
     * @param        $values
     * @param string $boolean
     *
     * @return self
     */
    public function orHavingNotIn($column, $values, $boolean = Operator::OR) : self
    {
        return $this->havingNotIn($column, $values, $boolean);
    }

    /**
     * Add having statement with BETWEEN simulation.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function havingBetween($column, array $values, $boolean = Operator::AND, $not = false) : self
    {
        $operator = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->having($column, $operator, [$values[0], $values[1]], $boolean);
    }

    /**
     * Add having statement with BETWEEN simulation, but with column names as value.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     * @param bool   $not
     *
     * @return self
     */
    public function havingBetweenColumns($column, array $values, $boolean = Operator::AND, $not = false) : self
    {
        $type = $not ? Operator::NOT_BETWEEN : Operator::BETWEEN;

        return $this->having($column, $type, [new Identifier($values[0]), new Identifier($values[1])], $boolean);
    }

    /**
     * Add having statement with BETWEEN simulation, but with column names as value and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orHavingBetweenColumns($column, array $values) : self
    {
        return $this->havingBetweenColumns($column, $values, Operator::OR);
    }

    /**
     * Add having statement with BETWEEN simulation and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orHavingBetween($column, array $values) : self
    {
        return $this->havingBetween($column, $values, Operator::OR);
    }

    /**
     * Add having statement with NOT BETWEEN simulation.
     *
     * @param        $column
     * @param array  $values
     * @param string $boolean
     *
     * @return self
     */
    public function havingNotBetween($column, array $values, $boolean = Operator::AND) : self
    {
        return $this->havingBetween($column, $values, $boolean, true);
    }

    /**
     * Add having statement with NOT BETWEEN simulation and OR operator.
     *
     * @param       $column
     * @param array $values
     *
     * @return self
     */
    public function orHavingNotBetween($column, array $values) : self
    {
        return $this->havingNotBetween($column, $values, Operator::OR);
    }

    /**
     * Add dictionary value to select statement.
     *
     * @param string       $dict
     * @param string       $attribute
     * @param array|string $key
     * @param string       $as
     *
     * @return self
     */
    public function addSelectDict(string $dict, string $attribute, $key, string $as = null) : self
    {
        if (is_null($as)) {
            $as = $attribute;
        }

        $id = is_array($key) ? 'tuple('.implode(', ', array_map([$this->grammar, 'wrap'], $key)).')' : $this->grammar->wrap($key);

        return $this
            ->addSelect(new Expression("dictGetString('{$dict}', '{$attribute}', {$id}) as `{$as}`"));
    }

    /**
     * Add where on dictionary value in where statement.
     *
     * @param              $dict
     * @param              $attribute
     * @param array|string $key
     * @param              $operator
     * @param              $value
     * @param string       $concatOperator
     *
     * @return self
     */
    public function whereDict(
        string $dict,
        string $attribute,
        $key,
        $operator = null,
        $value = null,
        string $concatOperator = Operator::AND
    ) : self {
        $this->addSelectDict($dict, $attribute, $key);

        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 4);

        return $this->where($attribute, $operator, $value, $concatOperator);
    }

    /**
     * Add where on dictionary value in where statement and OR operator.
     *
     * @param $dict
     * @param $attribute
     * @param $key
     * @param $operator
     * @param $value
     *
     * @return self
     */
    public function orWhereDict(
        string $dict,
        string $attribute,
        $key,
        $operator = null,
        $value = null
    ) : self {
        list($value, $operator) = $this->prepareValueAndOperator($value, $operator, func_num_args() == 4);

        return $this->whereDict($dict, $attribute, $key, $operator, $value, Operator::OR);
    }

    /**
     * Add request which must be runned asynchronous.
     *
     * @param Closure|self|null $asyncQueries
     *
     * @return self
     */
    public function asyncWithQuery($asyncQueries = null) : self
    {
        if (is_null($asyncQueries)) {
            return $this->async[] = $this->newQuery();
        }

        if ($asyncQueries instanceof Closure) {
            $asyncQueries = tap($this->newQuery(), $asyncQueries);
        }

        if ($asyncQueries instanceof self) {
            $this->async[] = $asyncQueries;
        } else {
            throw new \InvalidArgumentException('Argument for async method must be Closure, Builder or nothing');
        }

        return $this;
    }

    /**
     * Add limit statement.
     *
     * @param int      $limit
     * @param int|null $offset
     *
     * @return self
     */
    public function limit(int $limit, int $offset = null) : self
    {
        $this->limit = new Limit($limit, $offset);

        return $this;
    }

    /**
     * Add limit n by statement.
     *
     * @param int   $count
     * @param array ...$columns
     *
     * @return self
     */
    public function limitBy(int $count, ...$columns) : self
    {
        $columns = isset($columns[0]) && is_array($columns[0]) ? $columns[0] : $columns;

        $this->limitBy = new Limit($count, null, $this->processColumns($columns, false));

        return $this;
    }

    /**
     * Alias for limit method.
     *
     * @param int      $limit
     * @param int|null $offset
     *
     * @return self
     */
    public function take(int $limit, int $offset = null) : self
    {
        return $this->limit($limit, $offset);
    }

    /**
     * Alias for limitBy method.
     *
     * @param int   $count
     * @param array ...$columns
     *
     * @return self
     */
    public function takeBy(int $count, ...$columns) : self
    {
        return $this->limitBy($count, ...$columns);
    }

    /**
     * Add group by statement.
     *
     * @param $columns
     *
     * @return self
     */
    public function groupBy(...$columns) : self
    {
        $columns = isset($columns[0]) && is_array($columns[0]) ? $columns[0] : $columns;

        if (empty($columns)) {
            $columns[] = '*';
        }

        $this->groups = $this->processColumns($columns, false);

        return $this;
    }

    /**
     * Add order by statement.
     *
     * @param string|Closure $column
     * @param string         $direction
     * @param string|null    $collate
     *
     * @return self
     */
    public function orderBy($column, string $direction = 'asc', string $collate = null) : self
    {
        $column = $this->processColumns([$column], false)[0];

        $direction = new OrderDirection(strtoupper($direction));

        $this->orders[] = [$column, $direction, $collate];

        return $this;
    }

    /**
     * Add order by statement "as is".
     *
     * @param string $expression
     *
     * @return self
     */
    public function orderByRaw(string $expression) : self
    {
        $column = $this->processColumns([new Expression($expression)], false)[0];
        $this->orders[] = [$column, null, null];

        return $this;
    }

    /**
     * Add ASC order statement.
     *
     * @param             $column
     * @param string|null $collate
     *
     * @return self
     */
    public function orderByAsc($column, string $collate = null) : self
    {
        return $this->orderBy($column, OrderDirection::ASC, $collate);
    }

    /**
     * Add DESC order statement.
     *
     * @param             $column
     * @param string|null $collate
     *
     * @return self
     */
    public function orderByDesc($column, string $collate = null) : self
    {
        return $this->orderBy($column, OrderDirection::DESC, $collate);
    }

    /**
     * Set query result format.
     *
     * @param string $format
     *
     * @return self
     */
    public function format(string $format) : self
    {
        $this->format = new Format(strtoupper($format));

        return $this;
    }

    /**
     * Get the SQL representation of the query.
     *
     * @return string
     */
    public function toSql() : string
    {
        return $this->grammar->compileSelect($this);
    }

    /**
     * Get an array of the SQL queries from all added async builders.
     *
     * @return array
     */
    public function toAsyncSqls() : array
    {
        return $this->grammar->compileAsyncQueries($this);
    }

    /**
     * Get columns for select statement.
     *
     * @return array
     */
    public function getColumns() : array
    {
        return $this->columns;
    }

    /**
     * Get order statements.
     *
     * @return array
     */
    public function getOrders() : array
    {
        return $this->orders;
    }

    /**
     * Get group statements.
     *
     * @return array
     */
    public function getGroups() : array
    {
        return $this->groups;
    }

    /**
     * Get having statements.
     *
     * @return array
     */
    public function getHavings() : array
    {
        return $this->havings;
    }

    /**
     * Get prewhere statements.
     *
     * @return array
     */
    public function getPreWheres() : array
    {
        return $this->prewheres;
    }

    /**
     * Get where statements.
     *
     * @return array
     */
    public function getWheres() : array
    {
        return $this->wheres;
    }

    /**
     * Get From object.
     *
     * @return From|null
     */
    public function getFrom() : ?From
    {
        return $this->from;
    }

    /**
     * Get JoinClause.
     *
     * @return JoinClause
     */
    public function getJoin() : ?JoinClause
    {
        return $this->join;
    }

    /**
     * Get limit statement.
     *
     * @return Limit
     */
    public function getLimit() : ?Limit
    {
        return $this->limit;
    }

    /**
     * Get limit by statement.
     *
     * @return Limit
     */
    public function getLimitBy() : ?Limit
    {
        return $this->limitBy;
    }

    /**
     * Get sample statement.
     *
     * @return float|null
     */
    public function getSample() : ?float
    {
        return $this->sample;
    }

    /**
     * Get query unions.
     *
     * @return array
     */
    public function getUnions() : array
    {
        return $this->unions;
    }

    /**
     * Get format.
     *
     * @return null|Format
     */
    public function getFormat() : ?Format
    {
        return $this->format;
    }

    /**
     * Get query builders which must be run asynchronous.
     *
     * @return array
     */
    public function getAsync() : array
    {
        return $this->async;
    }
}
