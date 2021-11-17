<?php

namespace Tinderbox\ClickhouseBuilder\Query\Enums;

use MyCLabs\Enum\Enum;

/**
 * Operators.
 */
final class Operator extends Enum
{
    public const EQUALS = '=';
    public const NOT_EQUALS = '!=';
    public const LESS_OR_EQUALS = '<=';
    public const GREATER_OR_EQUALS = '>=';
    public const LESS = '<';
    public const GREATER = '>';
    public const LIKE = 'LIKE';
    public const ILIKE = 'ILIKE';
    public const NOT_LIKE = 'NOT LIKE';
    public const BETWEEN = 'BETWEEN';
    public const NOT_BETWEEN = 'NOT BETWEEN';
    public const IN = 'IN';
    public const NOT_IN = 'NOT IN';
    public const GLOBAL_IN = 'GLOBAL IN';
    public const GLOBAL_NOT_IN = 'GLOBAL NOT IN';
    public const AND = 'AND';
    public const OR = 'OR';
    public const CONCAT = '||';
    public const LAMBDA = '->';
    public const DIVIDE = '/';
    public const MODULO = '%';
    public const MULTIPLE = '*';
    public const PLUS = '+';
    public const MINUS = '-';
}
