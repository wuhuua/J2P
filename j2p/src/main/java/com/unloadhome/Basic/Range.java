package com.unloadhome.Basic;

import java.util.Iterator;

public class Range implements Iterable<Integer> {
    private final Integer DEFAULT_START = 0;

    private final Integer DEFAULT_GAP = 1;

    private class RangeIterator implements Iterator<Integer> {
        private final Integer inputVal;

        private Integer curVal;

        private final Integer gap;

        RangeIterator(Integer inputVal) {
            this.inputVal = inputVal;
            this.curVal = DEFAULT_START;
            this.gap = DEFAULT_GAP;
        }

        RangeIterator(Integer gap, Integer inputVal) {
            this.inputVal = inputVal;
            this.curVal = DEFAULT_START;
            this.gap = gap;
        }

        RangeIterator(Integer startVal, Integer gap, Integer inputVal) {
            this.inputVal = inputVal;
            this.curVal = startVal;
            this.gap = gap;
        }

        @Override
        public boolean hasNext() {
            return curVal < inputVal;
        }

        @Override
        public Integer next() {
            Integer tmpVal = curVal;
            curVal += gap;
            return tmpVal;
        }
    }

    private final RangeIterator rangeIterator;

    public Range(Integer inputVal) {
        this.rangeIterator = new RangeIterator(inputVal);
    }

    public Range(Integer gap, Integer inputVal) {
        this.rangeIterator = new RangeIterator(gap, inputVal);
    }

    public Range(Integer startVal, Integer gap, Integer inputVal) {
        this.rangeIterator = new RangeIterator(startVal, gap, inputVal);
    }

    @Override
    public Iterator<Integer> iterator() {
        return this.rangeIterator;
    }

}
