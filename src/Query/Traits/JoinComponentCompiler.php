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
     * @param Builder      $query
     * @param JoinClause[] $joins
     *
     * @throws GrammarException
     *
     * @return string
     */
    protected function compileJoinsComponent(Builder $query, array $joins): string
    {
        $result = [];

        foreach ($joins as $join) {
            $this->verifyJoin($join);

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
            if (!is_null($join->getUsing())) {
                $result[] = 'USING';
                $result[] = implode(', ', array_map(function ($column) {
                    return $this->wrap($column);
                }, $join->getUsing()));
            } else {
                $result[] = 'ON';
                $result[] = $this->compileTwoElementLogicExpressions($join->getOnClauses());
            }
        }

        return implode(' ', $result);
    }

    /**
     * Verifies join.
     *
     * @param JoinClause $joinClause
     *
     * @throws GrammarException
     */
    private function verifyJoin(JoinClause $joinClause): void
    {
        if (
            is_null($joinClause->getTable()) ||
            (is_null($joinClause->getUsing()) && is_null($joinClause->getOnClauses()))
        ) {
            throw GrammarException::wrongJoin($joinClause);
        } elseif (!is_null($joinClause->getUsing()) && !is_null($joinClause->getOnClauses())) {
            throw GrammarException::ambiguousJoinKeys();
        }
    }
}
