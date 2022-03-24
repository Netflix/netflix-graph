package com.netflix.nfgraph.build;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.util.ByteUtils;
import it.unimi.dsi.fastutil.ints.Int2LongAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.IntBidirectionalIterator;

public class NFWeightedBuildGraphOrdinalIterator extends WeightedOrdinalIterator {
    private final Int2LongAVLTreeMap ordinalWithWeight;
    private IntBidirectionalIterator iterator;

    NFWeightedBuildGraphOrdinalIterator(Int2LongMap ordinalWithWeight) {
        this.ordinalWithWeight = new Int2LongAVLTreeMap();
        if (null != ordinalWithWeight) {
            this.ordinalWithWeight.putAll(ordinalWithWeight);
            this.iterator = this.ordinalWithWeight.keySet().iterator();
        }
    }

    public NFWeightedBuildGraphOrdinalIterator(Int2LongAVLTreeMap ordinalWithWeight) {
        this.ordinalWithWeight = ordinalWithWeight;
    }

    @Override
    public void reset() {
        this.iterator = this.ordinalWithWeight.keySet().iterator(0);
    }

    @Override
    public int nextOrdinal() {
        return nextOrdinalWithWeightInMap()[0][0];
    }

    @Override
    public OrdinalIterator copy() {
        return new NFWeightedBuildGraphOrdinalIterator(ordinalWithWeight);
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public int[][] nextOrdinalWithWeightAndLabel() {
        return nextOrdinalWithWeightInMap();
    }

    @Override
    public int[][] nextOrdinalWithWeight() {
        int[][] ordinalWithWeightAndLabel = nextOrdinalWithWeightInMap();
        return new int[][]{{ordinalWithWeightAndLabel[0][0], ordinalWithWeightAndLabel[0][1]}};
    }

    @Override
    public int[][] nextOrdinalWithLabel() {
        int[][] ordinalWithWeightAndLabel = nextOrdinalWithWeightInMap();
        return new int[][]{{ordinalWithWeightAndLabel[0][0], ordinalWithWeightAndLabel[0][2]}};
    }

    private int[][] nextOrdinalWithWeightInMap() {
        if (iterator == null || !iterator.hasNext()) return WeightedOrdinalIterator.NO_MORE_DATA;
        int ordinal = iterator.nextInt();
        int weight = INVALID_WEIGHTS;
        int label = INVALID_LABEL;
        if (ordinalWithWeight.containsKey(ordinal)) {
            long value = ordinalWithWeight.get(ordinal);
            weight = ByteUtils.getLeftInt(value);
            label = ByteUtils.getRightInt(value);
        }
        return new int[][]{{ordinal, weight, label}};
    }
}
