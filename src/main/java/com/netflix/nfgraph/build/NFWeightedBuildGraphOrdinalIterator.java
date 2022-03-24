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

    private NFWeightedBuildGraphOrdinalIterator(Int2LongAVLTreeMap ordinalWithWeight) {
        this.ordinalWithWeight = ordinalWithWeight;
        this.iterator = this.ordinalWithWeight.keySet().iterator();
    }

    @Override
    public void reset() {
        this.iterator = this.ordinalWithWeight.keySet().iterator();
    }

    @Override
    public int nextOrdinal() {
        return nextOrdinalWithWeightInMap()[0];
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
    public int[] nextOrdinalWithWeightAndProperty() {
        return nextOrdinalWithWeightInMap();
    }

    @Override
    public int[] nextOrdinalWithWeight() {
        int[] ordinalWithWeightAndProperty = nextOrdinalWithWeightInMap();
        return new int[]{ordinalWithWeightAndProperty[0], ordinalWithWeightAndProperty[1]};
    }

    @Override
    public int[] nextOrdinalWithProperty() {
        int[] ordinalWithWeightAndProperty = nextOrdinalWithWeightInMap();
        return new int[]{ordinalWithWeightAndProperty[0], ordinalWithWeightAndProperty[2]};
    }

    private int[] nextOrdinalWithWeightInMap() {
        if (iterator == null || !iterator.hasNext()) return WeightedOrdinalIterator.NO_MORE_DATA;
        int ordinal = iterator.nextInt();
        int weight = INVALID_WEIGHTS;
        int property = INVALID_PROPERTY;
        if (ordinalWithWeight.containsKey(ordinal)) {
            long value = ordinalWithWeight.get(ordinal);
            weight = ByteUtils.getLeftInt(value);
            property = ByteUtils.getRightInt(value);
        }
        return new int[]{ordinal, weight, property};
    }
}
