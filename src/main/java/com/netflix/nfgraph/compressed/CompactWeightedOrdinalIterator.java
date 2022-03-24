package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.util.ByteArrayReader;

public class CompactWeightedOrdinalIterator extends WeightedOrdinalIterator {
    private final ByteArrayReader arrayReader;
    private int currentOrdinal = 0;
    private int currentWeight = 0;
    private int currentLabel = 0;

    public CompactWeightedOrdinalIterator(ByteArrayReader arrayReader) {
        this.arrayReader = arrayReader;
    }

    @Override
    public int nextOrdinal() {
        int delta = arrayReader.readVInt();
        if (delta == -1) {
            return NO_MORE_ORDINALS;
        }
        currentOrdinal += delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return NO_MORE_ORDINALS;
        }
        currentWeight = delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return NO_MORE_ORDINALS;
        }
        currentLabel = delta;
        return currentOrdinal;
    }

    @Override
    public void reset() {
        arrayReader.reset();
        currentOrdinal = 0;
        currentWeight = 0;
        currentLabel = 0;
    }

    @Override
    public OrdinalIterator copy() {
        return new CompactWeightedOrdinalIterator(arrayReader.copy());
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

    @Override
    public int[][] nextOrdinalWithWeightAndLabel() {
        int delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentOrdinal += delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentWeight = delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentLabel = delta;
        return new int[][]{{currentOrdinal, currentWeight, currentLabel}};
    }

    @Override
    public int[][] nextOrdinalWithWeight() {
        int delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentOrdinal += delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentWeight = delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentLabel = delta;
        return new int[][]{{currentOrdinal, currentWeight}};
    }

    @Override
    public int[][] nextOrdinalWithLabel() {
        int delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentOrdinal += delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentWeight = delta;
        delta = arrayReader.readVInt();
        if (delta == -1) {
            return WeightedOrdinalIterator.NO_MORE_DATA;
        }
        currentLabel = delta;
        return new int[][]{{currentOrdinal, currentLabel}};
    }
}
