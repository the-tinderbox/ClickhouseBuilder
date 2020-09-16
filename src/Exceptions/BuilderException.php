<?php

namespace Tinderbox\ClickhouseBuilder\Exceptions;

class BuilderException extends Exception
{
    public static function cannotDetermineAliasForColumn()
    {
        return new static('Cannot determine alias for the column');
    }

    public static function noTableStructureProvided()
    {
        return new static('No structure provided for insert in memory table');
    }
}
