package com.netflix.nfgraph.build;

import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

import java.util.Arrays;

public class NFWeightedBuildGraphNode {

    private final NFNodeSpec nodeSpec;
    private final int ordinal;
    private NFWeightedBuildGraphNodeConnections[] connectionModelSpecificConnections;
    private int numIncomingConnections;
    private int numOutgoingConnections;

    NFWeightedBuildGraphNode(NFNodeSpec nodeSpec, int ordinal, int numKnownConnectionModels) {
        this.nodeSpec = nodeSpec;
        this.connectionModelSpecificConnections = new NFWeightedBuildGraphNodeConnections[numKnownConnectionModels];
        this.ordinal = ordinal;
        this.numIncomingConnections = 0;
        this.numOutgoingConnections = 0;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public int getConnection(int connectionModelIndex, NFPropertySpec spec) {
        NFWeightedBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if (connections == null) {
            return -1;
        }
        return connections.getConnection(spec);
    }

    int getConnectionWeight(int connectionModelIndex, NFPropertySpec spec, int toNode) {
        NFWeightedBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if (connections == null) {
            return WeightedOrdinalIterator.INVALID_WEIGHTS;
        }
        return connections.getConnectionWeight(spec, toNode);
    }

    int getConnectionProperty(int connectionModelIndex, NFPropertySpec spec, int toNode) {
        NFWeightedBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if (connections == null) {
            return WeightedOrdinalIterator.INVALID_LABEL;
        }
        return connections.getConnectionProperty(spec, toNode);
    }

    public WeightedOrdinalSet getConnectionSet(int connectionModelIndex, NFPropertySpec spec) {
        NFWeightedBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if (connections == null) {
            return WeightedOrdinalSet.EMPTY_SET;
        }
        return connections.getConnectionSet(spec);
    }

    public WeightedOrdinalIterator getConnectionIterator(int connectionModelIndex, NFPropertySpec spec) {
        NFWeightedBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if (connections == null) {
            return WeightedOrdinalIterator.EMPTY_WEIGHTED_ITERATOR;
        }
        return connections.getConnectionIterator(spec);
    }

    void addConnection(int connectionModelIndex, NFPropertySpec spec, int ordinal, int weight) {
        NFWeightedBuildGraphNodeConnections connections = getOrCreateConnections(connectionModelIndex);
        connections.addConnection(spec, ordinal, weight);
    }

    void addConnection(int connectionModelIndex, NFPropertySpec spec, int ordinal, int weight, int label) {
        NFWeightedBuildGraphNodeConnections connections = getOrCreateConnections(connectionModelIndex);
        connections.addConnection(spec, ordinal, weight, label);
    }

    void incrementNumIncomingConnections() {
        numIncomingConnections++;
    }

    public int getNumIncomingConnections() {
        return numIncomingConnections;
    }

    void incrementNumOutgoingConnections() {
        numOutgoingConnections++;
    }

    public int getNumOutgoingConnections() {
        return numOutgoingConnections;
    }

    private NFWeightedBuildGraphNodeConnections getConnections(int connectionModelIndex) {
        if (connectionModelSpecificConnections.length <= connectionModelIndex) {
            return null;
        }
        return connectionModelSpecificConnections[connectionModelIndex];
    }

    private NFWeightedBuildGraphNodeConnections getOrCreateConnections(int connectionModelIndex) {
        if (connectionModelSpecificConnections.length <= connectionModelIndex) {
            connectionModelSpecificConnections = Arrays.copyOf(connectionModelSpecificConnections, connectionModelIndex + 1);
        }

        if (connectionModelSpecificConnections[connectionModelIndex] == null) {
            connectionModelSpecificConnections[connectionModelIndex] = new NFWeightedBuildGraphNodeConnections(nodeSpec);
        }
        return connectionModelSpecificConnections[connectionModelIndex];
    }

}
