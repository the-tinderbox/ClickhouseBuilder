<?php

if (!function_exists('tap')) {
    /**
     * Call the given Closure with the given value then return the value.
     *
     * @param mixed    $value
     * @param callable $callback
     *
     * @return mixed
     */
    function tap($value, $callback)
    {
        $callback($value);

        return $value;
    }
}

if (!function_exists('array_flatten')) {
    /**
     * Flatten a multi-dimensional array into a single level.
     *
     * @param array $array
     * @param int   $depth
     *
     * @return array
     */
    function array_flatten($array, $depth = INF) : array
    {
        return array_reduce($array, function ($result, $item) use ($depth) {
            if (!is_array($item)) {
                return array_merge($result, [$item]);
            } elseif ($depth === 1) {
                return array_merge($result, array_values($item));
            } else {
                return array_merge($result, array_flatten($item, $depth - 1));
            }
        }, []);
    }
}

if (!function_exists('raw')) {
    /**
     * Wrap string into Expression object for inserting in sql query as is.
     *
     * @param string $expr
     *
     * @return \Tinderbox\ClickhouseBuilder\Query\Expression
     */
    function raw(string $expr) : \Tinderbox\ClickhouseBuilder\Query\Expression
    {
        return new \Tinderbox\ClickhouseBuilder\Query\Expression($expr);
    }
}
