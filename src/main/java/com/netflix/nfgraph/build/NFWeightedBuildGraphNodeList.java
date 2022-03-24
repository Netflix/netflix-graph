package com.netflix.nfgraph.build;

import java.util.ArrayList;

public class NFWeightedBuildGraphNodeList {
    private final ArrayList<NFWeightedBuildGraphNode> list;

    NFWeightedBuildGraphNodeList() {
        list = new ArrayList<>();
    }

    public NFWeightedBuildGraphNode get(int ordinal) {
        return list.get(ordinal);
    }

    boolean add(NFWeightedBuildGraphNode node) {
        return list.add(node);
    }

    public int size() {
        return list.size();
    }

    NFWeightedBuildGraphNode set(int ordinal, NFWeightedBuildGraphNode node) {
        return list.set(ordinal, node);
    }
}
