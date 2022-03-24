package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;

public class SingleWeightedOrdinalSet extends WeightedOrdinalSet {
    private final int ordinal;
    private final int weight;
    private final int property;

    public SingleWeightedOrdinalSet(int ordinal, int weight, int property) {
        this.ordinal = ordinal;
        this.weight = weight;
        this.property = property;
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
        return new SingleWeightedOrdinalIterator(ordinal, weight, property);
    }

    @Override
    public int size() {
        return 1;
    }

    @Override
    public int[][] asArrayWithWeightAndProperty() {
        return new int[][]{{ordinal, weight, property}};
    }

    @Override
    public int[][] asArrayWithWeight() {
        return new int[][]{{ordinal, weight}};
    }

    @Override
    public int[][] asArrayWithProperty() {
        return new int[][]{{ordinal, property}};
    }
}
