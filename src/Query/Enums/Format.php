<?php

namespace Tinderbox\ClickhouseBuilder\Query\Enums;

use MyCLabs\Enum\Enum;

/**
 * Formats.
 */
final class Format extends Enum
{
    public const BLOCK_TAB_SEPARATED = 'BlockTabSeparated';
    public const CSV = 'CSV';
    public const CSV_WITH_NAMES = 'CSVWithNames';
    public const JSON = 'JSON';
    public const JSON_COMPACT = 'JSONCompact';
    public const JSON_EACH_ROW = 'JSONEachRow';
    public const NATIVE = 'Native';
    public const NULL = 'Null';
    public const PRETTY = 'Pretty';
    public const PRETTY_COMPACT = 'PrettyCompact';
    public const PRETTY_COMPACT_MONO_BLOCK = 'PrettyCompactMonoBlock';
    public const PRETTY_NO_ESCAPES = 'PrettyNoEscapes';
    public const PRETTY_COMPACT_NO_ESCAPES = 'PrettyCompactNoEscapes';
    public const PRETTY_SPACE_NO_ESCAPES = 'PrettySpaceNoEscapes';
    public const PRETTY_SPACE = 'PrettySpace';
    public const ROW_BINARY = 'RowBinary';
    public const TAB_SEPARATED = 'TabSeparated';
    public const TAB_SEPARATED_RAW = 'TabSeparatedRaw';
    public const TAB_SEPARATED_WITH_NAMES = 'TabSeparatedWithNames';
    public const TAB_SEPARATED_WITH_NAMES_AND_TYPES = 'TabSeparatedWithNamesAndTypes';
    public const TSKV = 'TSKV';
    public const VALUES = 'Values';
    public const VERTICAL = 'Vertical';
    public const XML = 'XML';
    public const TSV = 'TSV';
}
