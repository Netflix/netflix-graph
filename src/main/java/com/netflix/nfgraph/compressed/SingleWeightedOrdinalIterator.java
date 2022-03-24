package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalIterator;

public class SingleWeightedOrdinalIterator extends WeightedOrdinalIterator {
    private final int ordinal;
    private final int weight;
    private final int label;
    private boolean returned;

    public SingleWeightedOrdinalIterator(int ordinal, int weight, int label) {
        this.ordinal = ordinal;
        this.weight = weight;
        this.label = label;
    }

    @Override
    public int nextOrdinal() {
        if (returned) {
            return NO_MORE_ORDINALS;
        }
        returned = true;
        return ordinal;
    }

    @Override
    public void reset() {
        returned = false;
    }

    @Override
    public OrdinalIterator copy() {
        return new SingleWeightedOrdinalIterator(ordinal, weight, label);
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public int[][] nextOrdinalWithWeightAndLabel() {
        if (returned) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        returned = true;
        return new int[][]{{ordinal, weight, label}};
    }

    @Override
    public int[][] nextOrdinalWithWeight() {
        if (returned) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        returned = true;
        return new int[][]{{ordinal, weight}};
    }

    @Override
    public int[][] nextOrdinalWithLabel() {
        if (returned) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        returned = true;
        return new int[][]{{ordinal, label}};
    }
}
