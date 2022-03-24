package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;

public class SingleWeightedOrdinalSet extends WeightedOrdinalSet {
    private final int ordinal;
    private final int weight;
    private final int label;

    public SingleWeightedOrdinalSet(int ordinal, int weight, int label) {
        this.ordinal = ordinal;
        this.weight = weight;
        this.label = label;
    }

    @Override
    public boolean contains(int value) {
        return ordinal == value;
    }

    @Override
    public int[] asArray() {
        return new int[]{ordinal};
    }

    @Override
    public WeightedOrdinalIterator iterator() {
        return new SingleWeightedOrdinalIterator(ordinal, weight, label);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public int[][] asArrayWithWeightAndLabel() {
        return new int[][]{{ordinal, weight, label}};
    }

    @Override
    public int[][] asArrayWithWeight() {
        return new int[][]{{ordinal, weight}};
    }

    @Override
    public int[][] asArrayWithLabel() {
        return new int[][]{{ordinal, label}};
    }
}
