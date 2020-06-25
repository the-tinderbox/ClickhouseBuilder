<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Exceptions\GrammarException;
use Tinderbox\ClickhouseBuilder\Query\BaseBuilder as Builder;
use Tinderbox\ClickhouseBuilder\Query\JoinClause;

trait JoinComponentCompiler
{
    /**
     * Compiles join to string to pass this string in query.
     *
     * @param Builder    $query
     * @param JoinClause $join
     *
     * @return string
     */
    protected function compileJoinComponent(Builder $query, JoinClause $join): string
    {
        $this->verifyJoin($join);

        $result = [];

        if ($join->isDistributed()) {
            $result[] = 'GLOBAL';
        }

        if (!is_null($join->getStrict())) {
            $result[] = $join->getStrict();
        }

        if (!is_null($join->getType())) {
            $result[] = $join->getType();
        }

        $result[] = 'JOIN';
        $result[] = $this->wrap($join->getTable());
        if ($join->getAlias()) {
            $result[] = 'AS';
            $result[] = $this->wrap($join->getAlias());
        }
        $result[] = 'USING';
        $result[] = implode(', ', array_map(function ($column) {
            return $this->wrap($column);
        }, $join->getUsing()));

        return implode(' ', $result);
    }

    /**
     * Verifies join.
     *
     * @param JoinClause $joinClause
     *
     * @throws GrammarException
     */
    private function verifyJoin(JoinClause $joinClause)
    {
        if (
            is_null($joinClause->getTable()) ||
            is_null($joinClause->getUsing())
        ) {
            throw GrammarException::wrongJoin($joinClause);
        }
    }
}
