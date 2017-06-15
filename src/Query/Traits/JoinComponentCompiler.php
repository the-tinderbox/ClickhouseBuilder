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
    protected function compileJoinComponent(Builder $query, JoinClause $join) : string
    {
        $this->verifyJoin($join);

        $result = [];

        if ($join->isDistributed()) {
            $result[] = 'GLOBAL';
        }

        $result[] = $join->getStrict();
        $result[] = $join->getType();
        $result[] = 'JOIN';
        $result[] = $this->wrap($join->getTable());
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
            is_null($joinClause->getStrict()) ||
            is_null($joinClause->getType()) ||
            is_null($joinClause->getUsing()) ||
            is_null($joinClause->getTable()) ||
            is_null($joinClause->getUsing())
        ) {
            throw GrammarException::wrongJoin($joinClause);
        }
    }
}
