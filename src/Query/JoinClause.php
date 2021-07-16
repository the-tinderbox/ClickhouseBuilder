<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Closure;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinStrict;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinType;
use Tinderbox\ClickhouseBuilder\Query\Enums\Operator;

class JoinClause
{
    /**
     * GLOBAL option.
     *
     * @var bool
     */
    private $global = false;

    /**
     * Join strictness.
     *
     * @var JoinStrict|null
     */
    private $strict;

    /**
     * Join type.
     *
     * @var JoinType|null
     */
    private $type;

    /**
     * Table for join.
     *
     * @var string|Expression|null
     */
    private $table;

    /**
     * Column which used to join rows between tables.
     *
     * Требуется, что бы колонки с обоих сторон назывались одинаково.
     *
     * @var array|null
     */
    private $using;

    /**
     * Builder which initiated join.
     *
     * @var Builder
     */
    private $query;

    /**
     * Used for sub-query which executed not in callback.
     *
     * @var Builder|null
     */
    private $subQuery;

    /**
     * Join alias.
     *
     * @var Identifier
     */
    private $alias;

    /**
     * On clauses for joining rows between tables.
     *
     * @var TwoElementsLogicExpression[]|null
     */
    private $onClauses;

    /**
     * JoinClause constructor.
     *
     * @param BaseBuilder $query
     */
    public function __construct(BaseBuilder $query)
    {
        $this->query = $query;
    }

    /**
     * Set table for join.
     *
     * @param string|Expression $table
     *
     * @return JoinClause
     */
    public function table($table): self
    {
        if (is_string($table)) {
            list($table, $alias) = $this->decomposeJoinExpressionToTableAndAlias($table);

            if (!is_null($alias)) {
                $this->as($alias);
            }

            $table = new Identifier($table);
        } elseif ($table instanceof BaseBuilder) {
            $table = new Expression("({$table->toSql()})");
        }

        $this->table = $table;

        return $this;
    }

    /**
     * Set column to use for join rows.
     *
     * @param array ...$columns
     *
     * @return JoinClause
     */
    public function using(...$columns): self
    {
        $this->using = $this->stringsToIdentifiers(array_flatten($columns));

        return $this;
    }

    /**
     * Set "on" clause for join.
     *
     * @param string|Expression $first
     * @param string            $operator
     * @param string|Expression $second
     * @param string            $concatOperator
     *
     * @return JoinClause
     */
    public function on($first, string $operator, $second, string $concatOperator = Operator::AND): self
    {
        $expression = (new TwoElementsLogicExpression($this->query))
            ->firstElement(is_string($first) ? new Identifier($first) : $first)
            ->operator($operator)
            ->secondElement(is_string($second) ? new Identifier($second) : $second)
            ->concatOperator($concatOperator);

        $this->onClauses[] = $expression;

        return $this;
    }

    /**
     * Add column to using statement.
     *
     * @param string|array $columns
     *
     * @return JoinClause
     */
    public function addUsing(...$columns): self
    {
        $this->using = array_merge($this->using ?? [], $this->stringsToIdentifiers(array_flatten($columns)));

        return $this;
    }

    /**
     * Set join strictness.
     *
     * @param string $strict
     *
     * @return JoinClause
     */
    public function strict(string $strict): self
    {
        $this->strict = new JoinStrict(strtoupper($strict));

        return $this;
    }

    /**
     * Set join type.
     *
     * @param string $type
     *
     * @return JoinClause
     */
    public function type(string $type): self
    {
        $this->type = new JoinType(strtoupper($type));

        return $this;
    }

    /**
     * Set ALL strictness.
     *
     * @return JoinClause
     */
    public function all(): self
    {
        return $this->strict(JoinStrict::ALL);
    }

    /**
     * Set ANY strictness.
     *
     * @return JoinClause
     */
    public function any(): self
    {
        return $this->strict(JoinStrict::ANY);
    }

    /**
     * Set INNER join type.
     *
     * @return JoinClause
     */
    public function inner(): self
    {
        return $this->type(JoinType::INNER);
    }

    /**
     * Set LEFT join type.
     *
     * @return JoinClause
     */
    public function left(): self
    {
        return $this->type(JoinType::LEFT);
    }

    /**
     * Set GLOBAL option.
     *
     * @param bool $global
     *
     * @return JoinClause
     */
    public function distributed(bool $global = false): self
    {
        $this->global = $global;

        return $this;
    }

    /**
     * Set sub-query as table to select from.
     *
     * @param Closure|BaseBuilder|null $query
     *
     * @return JoinClause|BaseBuilder
     */
    public function query($query = null)
    {
        if (is_null($query)) {
            return $this->subQuery();
        }

        if ($query instanceof Closure) {
            $query = tap($this->query->newQuery(), $query);
        }

        if ($query instanceof BaseBuilder) {
            $this->table(new Expression("({$query->toSql()})"));
        }

        return $this;
    }

    /**
     * Get sub-query builder.
     *
     * @param string|null $alias
     *
     * @return BaseBuilder
     */
    public function subQuery(string $alias = null): BaseBuilder
    {
        if ($alias) {
            $this->as($alias);
        }

        return $this->subQuery = $this->query->newQuery();
    }

    /**
     * Set join alias.
     *
     * @param string $alias
     *
     * @return $this
     */
    public function as(string $alias): self
    {
        $this->alias = new Identifier($alias);

        return $this;
    }

    /**
     * Get using columns.
     *
     * @return array|null
     */
    public function getUsing(): ?array
    {
        return $this->using;
    }

    /**
     * Get on clauses.
     *
     * @return array|null
     */
    public function getOnClauses(): ?array
    {
        return $this->onClauses;
    }

    /**
     * Get flag to use or not to use GLOBAL option.
     *
     * @return bool
     */
    public function isDistributed(): bool
    {
        return $this->global;
    }

    /**
     * Get join strictness.
     *
     * @return JoinStrict|null
     */
    public function getStrict(): ?JoinStrict
    {
        return $this->strict;
    }

    /**
     * Get join type.
     *
     * @return JoinType|null
     */
    public function getType(): ?JoinType
    {
        return $this->type;
    }

    /**
     * Get sub-query.
     *
     * @return BaseBuilder|null
     */
    public function getSubQuery(): ?BaseBuilder
    {
        return $this->subQuery;
    }

    /**
     * Get table to select from.
     *
     * @return Expression|null|string
     */
    public function getTable()
    {
        return $this->table;
    }

    /**
     * Get alias.
     *
     * @return Identifier
     */
    public function getAlias(): ?Identifier
    {
        return $this->alias;
    }

    /**
     * Converts strings to Identifier objects.
     *
     * @param array $array
     *
     * @return array
     */
    private function stringsToIdentifiers(array $array): array
    {
        return array_map(
            function ($element) {
                if (is_string($element)) {
                    return new Identifier($element);
                } else {
                    return $element;
                }
            },
            $array
        );
    }

    /**
     * Tries to decompose string join expression to table name and alias.
     *
     * @param string $table
     *
     * @return array
     */
    private function decomposeJoinExpressionToTableAndAlias(string $table): array
    {
        if (strpos(strtolower($table), ' as ') !== false) {
            return array_map('trim', preg_split('/\s+as\s+/i', $table));
        }

        return [$table, null];
    }
}
