package com.netflix.nfgraph.build;

import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.compressed.SingleWeightedOrdinalIterator;
import com.netflix.nfgraph.compressed.SingleWeightedOrdinalSet;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteUtils;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;

/**
 * Represents the connections for a node in an {@link NFWeightedBuildGraph} for a single connection model.
 * <p>
 * It is unlikely that this class will need to be used externally.
 */
public class NFWeightedBuildGraphNodeConnections {

    private final Int2LongOpenHashMap[] singleOrdinalWithLabelAndWeight;
    private final Int2LongOpenHashMap[] multipleOrdinalWithLabelAndWeight;

    NFWeightedBuildGraphNodeConnections(NFNodeSpec nodeSpec) {
        singleOrdinalWithLabelAndWeight = new Int2LongOpenHashMap[nodeSpec.getNumSingleProperties()];
        multipleOrdinalWithLabelAndWeight = new Int2LongOpenHashMap[nodeSpec.getNumMultipleProperties()];
        for (int i = 0; i < nodeSpec.getNumMultipleProperties(); i++) {
            multipleOrdinalWithLabelAndWeight[i] = new Int2LongOpenHashMap(4);
        }
        for (int i = 0; i < nodeSpec.getNumSingleProperties(); i++) {
            singleOrdinalWithLabelAndWeight[i] = new Int2LongOpenHashMap(2);
        }
    }

    int getConnection(NFPropertySpec spec) {
        if (spec.isSingle()) {
            if (!singleOrdinalWithLabelAndWeight[spec.getPropertyIndex()].isEmpty()) {
                return singleOrdinalWithLabelAndWeight[spec.getPropertyIndex()].keySet().iterator().nextInt();
            }
            return -1;
        }
        if (!multipleOrdinalWithLabelAndWeight[spec.getPropertyIndex()].isEmpty()) {
            return multipleOrdinalWithLabelAndWeight[spec.getPropertyIndex()].size();
        }
        return -1;
    }

    int getConnectionWeight(NFPropertySpec spec, int toNode) {
        Int2LongOpenHashMap ordinalToConnection = getConnectionMap(spec);
        if (ordinalToConnection.isEmpty() || !ordinalToConnection.containsKey(toNode)) {
            return WeightedOrdinalIterator.INVALID_WEIGHTS;
        }
        return ByteUtils.getLeftInt(ordinalToConnection.get(toNode));
    }

    int getConnectionProperty(NFPropertySpec spec, int toNode) {
        Int2LongOpenHashMap ordinalToConnection = getConnectionMap(spec);
        if (ordinalToConnection.isEmpty() || !ordinalToConnection.containsKey(toNode)) {
            return WeightedOrdinalIterator.INVALID_LABEL;
        }
        return ByteUtils.getRightInt(ordinalToConnection.get(toNode));
    }

    private Int2LongOpenHashMap getConnectionMap(NFPropertySpec spec) {
        if (spec.isSingle()) {
            return singleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
        } else {
            return multipleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
        }
    }

    WeightedOrdinalSet getConnectionSet(NFPropertySpec spec) {
        if (spec.isMultiple()) {
            Int2LongOpenHashMap ordinalWithWeight = multipleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
            return new NFWeightedBuildGraphOrdinalSet(ordinalWithWeight, ordinalWithWeight.size());
        }
        int ordinal = -1;
        int weight = WeightedOrdinalIterator.INVALID_WEIGHTS;
        int label = WeightedOrdinalIterator.INVALID_LABEL;
        Int2LongOpenHashMap ordinalWithWeight = singleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
        if (!ordinalWithWeight.isEmpty()) {
            ordinal = ordinalWithWeight.keySet().iterator().nextInt();
            if (ordinalWithWeight.containsKey(ordinal)) {
                long value = ordinalWithWeight.get(ordinal);
                weight = ByteUtils.getLeftInt(value);
                label = ByteUtils.getRightInt(value);
            }
        }
        return new SingleWeightedOrdinalSet(ordinal, weight, label);
    }

    WeightedOrdinalIterator getConnectionIterator(NFPropertySpec spec) {
        if (spec.isMultiple()) {
            return new NFWeightedBuildGraphOrdinalIterator(multipleOrdinalWithLabelAndWeight[spec.getPropertyIndex()]);
        }
        int ordinal = -1;
        int weight = WeightedOrdinalIterator.INVALID_WEIGHTS;
        int label = WeightedOrdinalIterator.INVALID_LABEL;
        Int2LongOpenHashMap ordinalWithWeight = this.singleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
        if (!ordinalWithWeight.isEmpty()) {
            ordinal = ordinalWithWeight.keySet().iterator().nextInt();
            if (ordinalWithWeight.containsKey(ordinal)) {
                long value = ordinalWithWeight.get(ordinal);
                weight = ByteUtils.getLeftInt(value);
                label = ByteUtils.getRightInt(value);
            }
        }
        return new SingleWeightedOrdinalIterator(ordinal, weight, label);
    }

    void addConnection(NFPropertySpec spec, int ordinal, int weight) {
        addConnection(spec, ordinal, weight, WeightedOrdinalIterator.INVALID_LABEL);
    }

    void addConnection(NFPropertySpec spec, int ordinal, int weight, int label) {
        if (weight < 0) {
            throw new IllegalArgumentException(String.format("Invalid weight value %d", weight));
        }
        int w = weight;
        int l = WeightedOrdinalIterator.INVALID_LABEL;
        if (spec.isMultiple()) {
            Int2LongOpenHashMap int2LongOpenHashMap = multipleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
            if (int2LongOpenHashMap.containsKey(ordinal)) {
                long value = int2LongOpenHashMap.get(ordinal);
                w += ByteUtils.getLeftInt(value);
                l = ByteUtils.getRightInt(value);
            }
            if (label != WeightedOrdinalIterator.INVALID_LABEL) {
                l = label;
            }
            int2LongOpenHashMap.put(ordinal, ByteUtils.packTwoInts(w, l));
        } else {
            Int2LongOpenHashMap int2IntOpenHashMap = singleOrdinalWithLabelAndWeight[spec.getPropertyIndex()];
            if (int2IntOpenHashMap.containsKey(ordinal)) {
                long value = int2IntOpenHashMap.get(ordinal);
                w += ByteUtils.getLeftInt(value);
                l = ByteUtils.getRightInt(value);
            } else {
                int2IntOpenHashMap.clear();
            }
            if (label != WeightedOrdinalIterator.INVALID_LABEL) {
                l = label;
            }
            int2IntOpenHashMap.put(ordinal, ByteUtils.packTwoInts(w, l));
        }
    }
}
