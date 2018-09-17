<?php

namespace Tinderbox\ClickhouseBuilder\Exceptions;

use Tinderbox\ClickhouseBuilder\Query\JoinClause;

class GrammarException extends Exception
{
    public static function wrongJoin(JoinClause $joinClause)
    {
        $whatMissing = [];

        if (is_null($joinClause->getStrict())) {
            $whatMissing[] = 'strict';
        }

        if (is_null($joinClause->getType())) {
            $whatMissing[] = 'type';
        }

        if (is_null($joinClause->getTable())) {
            $whatMissing[] = 'table or subquery';
        }

        if (is_null($joinClause->getUsing())) {
            $whatMissing[] = 'using';
        }

        $whatMissing = implode(', ', $whatMissing);

        return new static("Missed required segments for 'JOIN' section. Missed: {$whatMissing}");
    }

    public static function wrongFrom($from)
    {
        return new static("Missed table or subquery for 'FROM' section.");
    }

    public static function missedTableForInsert()
    {
        return new static('Missed table for insert statement.');
    }

    public static function missedWhereForDelete()
    {
        return new static('Missed where section for delete statement.');
    }
}
