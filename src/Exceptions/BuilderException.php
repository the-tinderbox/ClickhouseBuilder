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
        return new static("No structure provided for insert in memory table");
    }
    
    public static function multipleUsingJoinsNotSupported()
    {
        return new static("Multiple joins with using clause is not supported by Clickhouse, use joinOn() instead");
    }
}
