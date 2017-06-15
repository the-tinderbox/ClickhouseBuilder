<?php

namespace Tinderbox\ClickhouseBuilder\Query\Enums;

use MyCLabs\Enum\Enum;

/**
 * Order directions.
 */
final class OrderDirection extends Enum
{
    public const ASC = 'ASC';
    public const DESC = 'DESC';
}
