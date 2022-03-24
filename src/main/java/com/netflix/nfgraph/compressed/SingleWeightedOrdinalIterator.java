package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalIterator;

public class SingleWeightedOrdinalIterator extends WeightedOrdinalIterator {
    private final int ordinal;
    private final int weight;
    private final int property;
    private boolean returned;

    public SingleWeightedOrdinalIterator(int ordinal, int weight, int property) {
        this.ordinal = ordinal;
        this.weight = weight;
        this.property = property;
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
        return new SingleWeightedOrdinalIterator(ordinal, weight, property);
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public int[] nextOrdinalWithWeightAndProperty() {
        if (returned) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        returned = true;
        return new int[]{ordinal, weight, property};
    }

    @Override
    public int[] nextOrdinalWithWeight() {
        if (returned) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        returned = true;
        return new int[]{ordinal, weight};
    }

    @Override
    public int[] nextOrdinalWithProperty() {
        if (returned) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        returned = true;
        return new int[]{ordinal, property};
    }
}
