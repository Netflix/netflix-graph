package com.netflix.nfgraph.build;

import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;

import java.util.HashMap;
import java.util.Map;

public class NFWeightedBuildGraphNodeCache {

    private final NFGraphSpec graphSpec;
    private final NFGraphModelHolder buildGraphModelHolder;
    private final Map<String, NFWeightedBuildGraphNodeList> nodesByOrdinal;

    NFWeightedBuildGraphNodeCache(NFGraphSpec graphSpec, NFGraphModelHolder modelHolder) {
        this.nodesByOrdinal = new HashMap<>();
        this.graphSpec = graphSpec;
        this.buildGraphModelHolder = modelHolder;
    }

    NFWeightedBuildGraphNode getNode(String nodeType, int ordinal) {
        NFWeightedBuildGraphNodeList nodes = getNodes(nodeType);
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);
        return getNode(nodes, nodeSpec, ordinal);
    }

    NFWeightedBuildGraphNode getNode(NFWeightedBuildGraphNodeList nodes, NFNodeSpec nodeSpec, int ordinal) {
        while (ordinal >= nodes.size()) {
            nodes.add(null);
        }

        NFWeightedBuildGraphNode node = nodes.get(ordinal);

        if (node == null) {
            node = new NFWeightedBuildGraphNode(nodeSpec, ordinal, buildGraphModelHolder.size());
            nodes.set(ordinal, node);
        }

        return node;
    }

    public int numNodes(String nodeType) {
        return getNodes(nodeType).size();
    }

    public NFWeightedBuildGraphNodeList getNodes(String nodeType) {
        NFWeightedBuildGraphNodeList nodes = nodesByOrdinal.get(nodeType);
        if (nodes == null) {
            nodes = new NFWeightedBuildGraphNodeList();
            nodesByOrdinal.put(nodeType, nodes);
        }
        return nodes;
    }
}
