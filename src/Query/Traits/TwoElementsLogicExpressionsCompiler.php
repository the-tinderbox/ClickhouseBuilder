<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Query\Column;
use Tinderbox\ClickhouseBuilder\Query\Tuple;
use Tinderbox\ClickhouseBuilder\Query\TwoElementsLogicExpression;

trait TwoElementsLogicExpressionsCompiler
{
    /**
     * Compiles TwoElementsLogicExpression elements
     *
     * Used in prewhere, where and having statements
     *
     * @param TwoElementsLogicExpression[] $wheres
     *
     * @return string
     */
    private function compileTwoElementLogicExpressions(array $wheres) : string
    {
        $result = [];

        foreach ($wheres as $where) {
            $firstElement = $where->getFirstElement();
            $secondElement = $where->getSecondElement();
            $operator = $where->getOperator();
            $concat = $where->getConcatenationOperator();

            if (!empty($result)) {
                $result[] = $concat;
            }

            if ($firstElement = $this->compileElement($firstElement)) {
                $result[] = $firstElement;
            }

            if (!is_null($operator)) {
                $result[] = $operator;
            }

            if ($secondElement = $this->compileElement($secondElement)) {
                $result[] = $secondElement;
            }
        }

        return implode(' ', $result);
    }

    /**
     * Compiles one element in TwoElementsLogicExpression
     *
     * @param mixed $element
     *
     * @return string
     */
    private function compileElement($element) : string
    {
        $result = [];

        if (is_array($element)) {
            $result[] = "({$this->compileTwoElementLogicExpressions($element)})";
        } elseif ($element instanceof TwoElementsLogicExpression) {
            $result[] = $this->compileTwoElementLogicExpressions([$element]);
        } elseif ($element instanceof Tuple) {
            $result[] = "({$this->compileTuple($element)})";
        } elseif ($element instanceof Column) {
            $result[] = $this->compileColumn($element);
        } elseif (! is_null($element)) {
            $result[] = $this->wrap($element);
        }

        return implode(' ', $result);
    }

}
