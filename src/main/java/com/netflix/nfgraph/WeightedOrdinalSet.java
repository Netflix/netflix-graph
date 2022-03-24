package com.netflix.nfgraph;

public abstract class WeightedOrdinalSet extends OrdinalSet {

    public static final int[][] EMPTY_ORDINAL_2D_ARRAY = new int[0][];
    public static final int[] EMPTY_ORDINAL_ARRAY = new int[0];
    /**
     * An empty <code>WeightedOrdinalSet</code>.
     */
    public static final WeightedOrdinalSet EMPTY_SET = new WeightedOrdinalSet() {
        @Override
        public int[][] asArrayWithWeightAndLabel() {
            return EMPTY_ORDINAL_2D_ARRAY;
        }

        @Override
        public int[][] asArrayWithLabel() {
            return EMPTY_ORDINAL_2D_ARRAY;
        }

        @Override
        public int[][] asArrayWithWeight() {
            return EMPTY_ORDINAL_2D_ARRAY;
        }

        @Override
        public boolean contains(int value) {
            return false;
        }

        @Override
        public int[] asArray() {
            return EMPTY_ORDINAL_ARRAY;
        }

        @Override
        public WeightedOrdinalIterator iterator() {
            return WeightedOrdinalIterator.EMPTY_WEIGHTED_ITERATOR;
        }

        @Override
        public int size() {
            return 0;
        }
    };

    public abstract WeightedOrdinalIterator iterator();

    public abstract int[][] asArrayWithWeightAndLabel();

    public abstract int[][] asArrayWithLabel();

    public abstract int[][] asArrayWithWeight();
}
