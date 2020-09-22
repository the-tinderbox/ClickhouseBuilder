<?php

namespace Tinderbox\ClickhouseBuilder\Query;

/**
 * Object to represent tuple.
 */
class Tuple
{
    /**
     * Tuple elements.
     *
     * @var array
     */
    private $elements = [];

    /**
     * Tuple constructor.
     *
     * @param array $elements
     */
    public function __construct(array $elements = [])
    {
        $this->elements = $elements;
    }

    /**
     * Get tuple elements.
     *
     * @return array
     */
    public function getElements(): array
    {
        return $this->elements;
    }

    /**
     * Add element to tuple.
     *
     * @param array ...$elements
     *
     * @return Tuple
     */
    public function addElements(...$elements): self
    {
        $this->elements = array_merge($this->elements, array_flatten($elements));

        return $this;
    }
}
