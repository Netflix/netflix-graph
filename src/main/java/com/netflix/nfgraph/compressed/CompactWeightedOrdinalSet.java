package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.util.ByteArrayReader;

import java.util.Arrays;

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;

public class CompactWeightedOrdinalSet extends WeightedOrdinalSet {
    private final ByteArrayReader reader;
    private int size = Integer.MIN_VALUE;

    public CompactWeightedOrdinalSet(ByteArrayReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean contains(int value) {
        CompactWeightedOrdinalIterator iter = iterator();
        int iterValue = iter.nextOrdinal();
        while (iterValue < value) {
            iterValue = iter.nextOrdinal();
        }
        return iterValue == value;
    }

    @Override
    public boolean containsAll(int... values) {
        WeightedOrdinalIterator iter = iterator();
        Arrays.sort(values);
        int valuesIndex = 0;
        int setValue = iter.nextOrdinal();
        while (valuesIndex < values.length) {
            if (setValue == values[valuesIndex]) {
                valuesIndex++;
            } else if (setValue < values[valuesIndex]) {
                setValue = iter.nextOrdinal();
            } else {
                break;
            }
        }
        return valuesIndex == values.length;
    }

    @Override
    public int size() {
        if (sizeIsUnknown()) {
            int count = countVInts(reader.copy());
            assert count % 3 == 0;
            size = count / 3;
        }
        return size;
    }

    private boolean sizeIsUnknown() {
        return size == Integer.MIN_VALUE;
    }

    private int countVInts(ByteArrayReader myReader) {
        int counter = 0;
        while (myReader.readVInt() >= 0) counter++;
        return counter;
    }

    @Override
    public CompactWeightedOrdinalIterator iterator() {
        return new CompactWeightedOrdinalIterator(reader.copy());
    }

    @Override
    public int[][] asArrayWithWeightAndProperty() {
        int[][] arr = new int[size()][3];
        CompactWeightedOrdinalIterator iter = iterator();
        int[] ordinalWithWeight = iter.nextOrdinalWithWeightAndProperty();
        int i = 0;
        while (ordinalWithWeight[0] != NO_MORE_ORDINALS) {
            arr[i] = ordinalWithWeight;
            ordinalWithWeight = iter.nextOrdinalWithWeightAndProperty();
            i++;
        }
        return arr;
    }

    @Override
    public int[][] asArrayWithWeight() {
        int[][] arr = new int[size()][2];
        CompactWeightedOrdinalIterator iter = iterator();
        int[] ordinalWithWeight = iter.nextOrdinalWithWeight();
        int i = 0;
        while (ordinalWithWeight[0] != NO_MORE_ORDINALS) {
            arr[i] = ordinalWithWeight;
            ordinalWithWeight = iter.nextOrdinalWithWeight();
            i++;
        }
        return arr;
    }

    @Override
    public int[][] asArrayWithProperty() {
        int[][] arr = new int[size()][2];
        CompactWeightedOrdinalIterator iter = iterator();
        int[] ordinalWithWeight = iter.nextOrdinalWithProperty();
        int i = 0;
        while (ordinalWithWeight[0] != NO_MORE_ORDINALS) {
            arr[i] = ordinalWithWeight;
            ordinalWithWeight = iter.nextOrdinalWithProperty();
            i++;
        }
        return arr;
    }
}
