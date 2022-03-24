package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.util.ByteArrayReader;

public class CompactWeightedOrdinalIterator extends WeightedOrdinalIterator {
    private final ByteArrayReader arrayReader;
    private int currentOrdinal = 0;
    private int currentWeight = 0;
    private int currentProperty = 0;

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
        currentProperty = delta;
        return currentOrdinal;
    }

    @Override
    public void reset() {
        arrayReader.reset();
        currentOrdinal = 0;
        currentWeight = 0;
        currentProperty = 0;
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
    public int[] nextOrdinalWithWeightAndProperty() {
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
        currentProperty = delta;
        return new int[]{currentOrdinal, currentWeight, currentProperty};
    }

    @Override
    public int[] nextOrdinalWithWeight() {
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
        currentProperty = delta;
        return new int[]{currentOrdinal, currentWeight};
    }

    @Override
    public int[] nextOrdinalWithProperty() {
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
        currentProperty = delta;
        return new int[]{currentOrdinal, currentProperty};
    }
}
