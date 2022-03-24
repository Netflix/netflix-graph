package com.netflix.nfgraph;

public abstract class WeightedOrdinalIterator implements OrdinalIterator {
    public static final int NO_MORE_WEIGHTS = Integer.MAX_VALUE;
    public static final int NO_MORE_PROPERTY = Integer.MAX_VALUE;
    public static final int INVALID_WEIGHTS = Integer.MIN_VALUE;
    public static final int INVALID_PROPERTY = 0;
    public static final int[] NO_MORE_DATA = {NO_MORE_ORDINALS, NO_MORE_WEIGHTS, NO_MORE_PROPERTY};
    /**
     * An iterator which always return <code>WeightedOrdinalIterator.NO_MORE_ORDINALS</code>
     */
    public static final WeightedOrdinalIterator EMPTY_WEIGHTED_ITERATOR = new WeightedOrdinalIterator() {
        @Override
        public int[] nextOrdinalWithWeightAndProperty() {
            return NO_MORE_DATA;
        }

        @Override
        public int[] nextOrdinalWithWeight() {
            return NO_MORE_DATA;
        }

        @Override
        public int[] nextOrdinalWithProperty() {
            return NO_MORE_DATA;
        }

        @Override
        public int nextOrdinal() {
            return NO_MORE_ORDINALS;
        }

        @Override
        public void reset() {
        }

        @Override
        public OrdinalIterator copy() {
            return this;
        }

        @Override
        public boolean isOrdered() {
            return true;
        }
    };

    public abstract int[] nextOrdinalWithWeightAndProperty();

    public abstract int[] nextOrdinalWithWeight();

    public abstract int[] nextOrdinalWithProperty();
}
