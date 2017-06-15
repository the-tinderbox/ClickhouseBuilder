<?php

namespace Tinderbox\ClickhouseBuilder\Query;

use Tinderbox\ClickhouseBuilder\Query\Enums\JoinStrict;
use Tinderbox\ClickhouseBuilder\Query\Enums\JoinType;

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
            $table = new Identifier($table);
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
     * Alias for using method.
     *
     * @param array ...$columns
     *
     * @return JoinClause
     */
    public function on(...$columns): self
    {
        return $this->using($columns);
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
    public function all()
    {
        return $this->strict(JoinStrict::ALL);
    }

    /**
     * Set ANY strictness.
     *
     * @return JoinClause
     */
    public function any()
    {
        return $this->strict(JoinStrict::ANY);
    }

    /**
     * Set INNER join type.
     *
     * @return JoinClause
     */
    public function inner()
    {
        return $this->type(JoinType::INNER);
    }

    /**
     * Set LEFT join type.
     *
     * @return JoinClause
     */
    public function left()
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
     * @param \Closure|Builder|null $query
     *
     * @return JoinClause|Builder
     */
    public function query($query = null)
    {
        if (is_null($query)) {
            return $this->subQuery();
        }

        if ($query instanceof \Closure) {
            $query = tap($this->query->newQuery(), $query);
        }

        if ($query instanceof Builder) {
            $this->table(new Expression("({$query->toSql()})"));
        }

        return $this;
    }

    /**
     * Get sub-query builder.
     *
     * @return Builder
     */
    public function subQuery(): Builder
    {
        return $this->subQuery = $this->query->newQuery();
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
     * @return Builder|null
     */
    public function getSubQuery(): ?Builder
    {
        return $this->subQuery;
    }

    /**
     * Get table to select from.
     *
     * @return Expression|null|Identifier
     */
    public function getTable()
    {
        return $this->table;
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
}
