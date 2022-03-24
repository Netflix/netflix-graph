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

    private final Int2LongOpenHashMap[] singleOrdinalWithWeightAndProperty;
    private final Int2LongOpenHashMap[] multipleOrdinalWithWeightAndProperty;

    NFWeightedBuildGraphNodeConnections(NFNodeSpec nodeSpec) {
        singleOrdinalWithWeightAndProperty = new Int2LongOpenHashMap[nodeSpec.getNumSingleProperties()];
        multipleOrdinalWithWeightAndProperty = new Int2LongOpenHashMap[nodeSpec.getNumMultipleProperties()];
        for (int i = 0; i < nodeSpec.getNumMultipleProperties(); i++) {
            multipleOrdinalWithWeightAndProperty[i] = new Int2LongOpenHashMap(4);
        }
        for (int i = 0; i < nodeSpec.getNumSingleProperties(); i++) {
            singleOrdinalWithWeightAndProperty[i] = new Int2LongOpenHashMap(2);
        }
    }

    int getConnection(NFPropertySpec spec) {
        if (spec.isSingle()) {
            if (!singleOrdinalWithWeightAndProperty[spec.getPropertyIndex()].isEmpty()) {
                return singleOrdinalWithWeightAndProperty[spec.getPropertyIndex()].keySet().iterator().nextInt();
            }
            return -1;
        }
        if (!multipleOrdinalWithWeightAndProperty[spec.getPropertyIndex()].isEmpty()) {
            return multipleOrdinalWithWeightAndProperty[spec.getPropertyIndex()].size();
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
            return WeightedOrdinalIterator.INVALID_PROPERTY;
        }
        return ByteUtils.getRightInt(ordinalToConnection.get(toNode));
    }

    private Int2LongOpenHashMap getConnectionMap(NFPropertySpec spec) {
        if (spec.isSingle()) {
            return singleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
        } else {
            return multipleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
        }
    }

    WeightedOrdinalSet getConnectionSet(NFPropertySpec spec) {
        if (spec.isMultiple()) {
            Int2LongOpenHashMap ordinalWithWeight = multipleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
            return new NFWeightedBuildGraphOrdinalSet(ordinalWithWeight, ordinalWithWeight.size());
        }
        int ordinal = -1;
        int weight = WeightedOrdinalIterator.INVALID_WEIGHTS;
        int property = WeightedOrdinalIterator.INVALID_PROPERTY;
        Int2LongOpenHashMap ordinalWithWeight = singleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
        if (!ordinalWithWeight.isEmpty()) {
            ordinal = ordinalWithWeight.keySet().iterator().nextInt();
            if (ordinalWithWeight.containsKey(ordinal)) {
                long value = ordinalWithWeight.get(ordinal);
                weight = ByteUtils.getLeftInt(value);
                property = ByteUtils.getRightInt(value);
            }
        }
        return new SingleWeightedOrdinalSet(ordinal, weight, property);
    }

    WeightedOrdinalIterator getConnectionIterator(NFPropertySpec spec) {
        if (spec.isMultiple()) {
            return new NFWeightedBuildGraphOrdinalIterator(multipleOrdinalWithWeightAndProperty[spec.getPropertyIndex()]);
        }
        int ordinal = -1;
        int weight = WeightedOrdinalIterator.INVALID_WEIGHTS;
        int property = WeightedOrdinalIterator.INVALID_PROPERTY;
        Int2LongOpenHashMap ordinalWithWeight = this.singleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
        if (!ordinalWithWeight.isEmpty()) {
            ordinal = ordinalWithWeight.keySet().iterator().nextInt();
            if (ordinalWithWeight.containsKey(ordinal)) {
                long value = ordinalWithWeight.get(ordinal);
                weight = ByteUtils.getLeftInt(value);
                property = ByteUtils.getRightInt(value);
            }
        }
        return new SingleWeightedOrdinalIterator(ordinal, weight, property);
    }

    void addConnection(NFPropertySpec spec, int ordinal, int weight) {
        addConnection(spec, ordinal, weight, WeightedOrdinalIterator.INVALID_PROPERTY);
    }

    void addConnection(NFPropertySpec spec, int ordinal, int weight, int property) {
        if (weight < 0) {
            throw new IllegalArgumentException(String.format("Invalid weight value %d", weight));
        }
        int w = weight;
        int l = WeightedOrdinalIterator.INVALID_PROPERTY;
        if (spec.isMultiple()) {
            Int2LongOpenHashMap int2LongOpenHashMap = multipleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
            if (int2LongOpenHashMap.containsKey(ordinal)) {
                long value = int2LongOpenHashMap.get(ordinal);
                w += ByteUtils.getLeftInt(value);
                l = ByteUtils.getRightInt(value);
            }
            if (property != WeightedOrdinalIterator.INVALID_PROPERTY) {
                l = property;
            }
            int2LongOpenHashMap.put(ordinal, ByteUtils.packTwoInts(w, l));
        } else {
            Int2LongOpenHashMap int2IntOpenHashMap = singleOrdinalWithWeightAndProperty[spec.getPropertyIndex()];
            if (int2IntOpenHashMap.containsKey(ordinal)) {
                long value = int2IntOpenHashMap.get(ordinal);
                w += ByteUtils.getLeftInt(value);
                l = ByteUtils.getRightInt(value);
            } else {
                int2IntOpenHashMap.clear();
            }
            if (property != WeightedOrdinalIterator.INVALID_PROPERTY) {
                l = property;
            }
            int2IntOpenHashMap.put(ordinal, ByteUtils.packTwoInts(w, l));
        }
    }
}
