package com.unloadhome.Basic;

import java.util.Iterator;

public class Range implements Iterable<Number> {
    private final Number DEFAULT_START = Integer.valueOf(0);

    private final Number DEFAULT_GAP = Integer.valueOf(1);

    private class RangeIterator implements Iterator<Number> {
        private final Number endVal;

        private final Number startVal;

        private Number curVal;

        private final Number gap;

        RangeIterator(Number endVal) {
            this.endVal = endVal;
            this.startVal = DEFAULT_START;
            this.curVal = DEFAULT_START;
            this.gap = DEFAULT_GAP;
        }

        RangeIterator(Number gap, Number endVal) {
            this.endVal = endVal;
            this.curVal = DEFAULT_START;
            this.startVal = DEFAULT_START;
            this.gap = gap;
        }

        RangeIterator(Number startVal, Number gap, Number endVal) {
            this.endVal = endVal;
            this.curVal = startVal;
            this.startVal = startVal;
            this.gap = gap;
        }

        @Override
        public boolean hasNext() {
            return curVal.doubleValue() < endVal.doubleValue();
        }

        @Override
        public Number next() {
            Number tmpVal = curVal;
            if (endVal instanceof Integer && startVal instanceof Integer && gap instanceof Integer) {
                int gValue = gap.intValue();
                int cValue = curVal.intValue();
                curVal = Integer.valueOf(cValue + gValue);
            } else {
                double gValue = gap.doubleValue();
                double cValue = curVal.doubleValue();
                curVal = Double.valueOf(cValue + gValue);
            }
            return tmpVal;
        }
    }

    private final RangeIterator rangeIterator;

    public Range(Number inputVal) {
        this.rangeIterator = new RangeIterator(inputVal);
    }

    public Range(Number gap, Number inputVal) {
        this.rangeIterator = new RangeIterator(gap, inputVal);
    }

    public Range(Number startVal, Number gap, Number inputVal) {
        this.rangeIterator = new RangeIterator(startVal, gap, inputVal);
    }

    @Override
    public Iterator<Number> iterator() {
        return this.rangeIterator;
    }

}
