<?php

namespace Tinderbox\ClickhouseBuilder\Query\Traits;

use Tinderbox\ClickhouseBuilder\Query\Column;
use Tinderbox\ClickhouseBuilder\Query\Tuple;
use Tinderbox\ClickhouseBuilder\Query\TwoElementsLogicExpression;

trait TwoElementsLogicExpressionsCompiler
{
    /**
     * Compiles TwoElementsLogicExpression elements.
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

            $result[] = $this->compileElement($firstElement);

            if (!is_null($operator)) {
                $result[] = $operator;
            }

            $result[] = $this->compileElement($secondElement);
        }

        return implode(' ', array_filter($result, function ($val) {
            return is_numeric($val) ? true : (bool) $val;
        }));
    }

    /**
     * Compiles one element in TwoElementsLogicExpression.
     *
     * @param mixed $element
     *
     * @return string|int
     */
    private function compileElement($element)
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
        } elseif (!is_null($element)) {
            $result[] = $this->wrap($element);
        }

        if (empty($result)) {
            return '';
        }

        return implode(' ', $result);
    }
}
