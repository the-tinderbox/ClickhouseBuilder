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
    
    public static function noTableStructureProvided()
    {
        return new static("No structure provided for insert in memory table");
    }
    
    public static function couldNotInstantiateFile()
    {
        return new static("Could not instantiate file");
    }
}
