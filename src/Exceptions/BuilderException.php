<?php

namespace Tinderbox\ClickhouseBuilder\Exceptions;

class BuilderException extends Exception
{
    public static function cannotDetermineAliasForColumn()
    {
        return new static('Cannot determine alias for the column');
    }

    public static function temporaryTableAlreadyExists($tableName)
    {
        return new static("Temporary table {$tableName} already exists in query");
    }
}
