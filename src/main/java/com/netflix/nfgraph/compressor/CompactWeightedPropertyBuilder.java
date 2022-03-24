package com.netflix.nfgraph.compressor;

import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.util.ByteArrayBuffer;

import java.util.Arrays;
import java.util.Comparator;

public class CompactWeightedPropertyBuilder {
    private final ByteArrayBuffer buf;

    public CompactWeightedPropertyBuilder(ByteArrayBuffer buf) {
        this.buf = buf;
    }

    public void buildProperty(WeightedOrdinalSet ordinalSet) {
        int[][] connectedOrdinals = ordinalSet.asArrayWithWeightAndLabel();
        Arrays.sort(connectedOrdinals, Comparator.comparingInt(o -> o[0]));
        int previousOrdinal = 0;
        for (int[] connectedOrdinal : connectedOrdinals) {
            // write ordinal
            buf.writeVInt(connectedOrdinal[0] - previousOrdinal);
            previousOrdinal = connectedOrdinal[0];
            // write weight
            buf.writeVInt(connectedOrdinal[1]);
            // write label
            buf.writeVInt(connectedOrdinal[2]);
        }
    }
}
