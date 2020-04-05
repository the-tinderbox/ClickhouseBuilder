<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Exceptions\GrammarException;
use Tinderbox\ClickhouseBuilder\Query\BaseBuilder as Builder;
use Tinderbox\ClickhouseBuilder\Query\JoinClause;

trait JoinsComponentCompiler
{
    use JoinCompiler;
    
    /**
     * Compiles join to string to pass this string in query.
     *
     * @param Builder      $query
     * @param JoinClause[] $joins
     * @return string
     * @throws GrammarException
     */
    public function compileJoinsComponent(Builder $query, array $joins): string
    {
        $result = [];
        
        foreach ($joins as $join) {
            $result [] = $this->compileJoin($query, $join);
        }
        
        return implode(' ', $result);
    }
}
