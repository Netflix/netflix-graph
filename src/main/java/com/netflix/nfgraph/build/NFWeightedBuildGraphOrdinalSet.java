package com.netflix.nfgraph.build;

import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.util.ByteUtils;
import it.unimi.dsi.fastutil.ints.Int2LongMap;

import java.util.Arrays;

public class NFWeightedBuildGraphOrdinalSet extends WeightedOrdinalSet {

    private final Int2LongMap ordinalWithWeight;
    private final int size;

    NFWeightedBuildGraphOrdinalSet(Int2LongMap ordinalWithWeight, int size) {
        this.ordinalWithWeight = ordinalWithWeight;
        this.size = size;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean contains(int value) {
        return this.ordinalWithWeight.containsKey(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int[] asArray() {
        return Arrays.copyOf(ordinalWithWeight.keySet().toIntArray(), size);
    }

    @Override
    public int[][] asArrayWithWeightAndProperty() {
        if (null == ordinalWithWeight) {
            return WeightedOrdinalSet.EMPTY_ORDINAL_2D_ARRAY;
        }
        int[][] ordinal = new int[ordinalWithWeight.size()][3];
        int i = 0;
        for (Int2LongMap.Entry entry : ordinalWithWeight.int2LongEntrySet()) {
            ordinal[i][0] = entry.getIntKey();
            ordinal[i][1] = ByteUtils.getLeftInt(entry.getLongValue());
            ordinal[i][2] = ByteUtils.getRightInt(entry.getLongValue());
            i++;
        }
        return ordinal;
    }

    @Override
    public int[][] asArrayWithWeight() {
        if (null == ordinalWithWeight) {
            return WeightedOrdinalSet.EMPTY_ORDINAL_2D_ARRAY;
        }
        int[][] ordinal = new int[ordinalWithWeight.size()][2];
        int i = 0;
        for (Int2LongMap.Entry entry : ordinalWithWeight.int2LongEntrySet()) {
            ordinal[i][0] = entry.getIntKey();
            ordinal[i][1] = ByteUtils.getLeftInt(entry.getLongValue());
            i++;
        }
        return ordinal;
    }

    @Override
    public int[][] asArrayWithProperty() {
        if (null == ordinalWithWeight) {
            return WeightedOrdinalSet.EMPTY_ORDINAL_2D_ARRAY;
        }
        int[][] ordinal = new int[ordinalWithWeight.size()][2];
        int i = 0;
        for (Int2LongMap.Entry entry : ordinalWithWeight.int2LongEntrySet()) {
            ordinal[i][0] = entry.getIntKey();
            ordinal[i][1] = ByteUtils.getRightInt(entry.getLongValue());
            i++;
        }
        return ordinal;
    }

    @Override
    public WeightedOrdinalIterator iterator() {
        return new NFWeightedBuildGraphOrdinalIterator(ordinalWithWeight);
    }

    @Override
    public int size() {
        return size;
    }
}
