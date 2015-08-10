package com.netflix.nfgraph.build;

import java.util.ArrayList;

/**
 * Encapsulates an ordered list of {@link com.netflix.nfgraph.build.NFBuildGraphNode}s.
 * @author ishastri
 */
public class NFBuildGraphNodeList {
    private ArrayList<NFBuildGraphNode> list;

    NFBuildGraphNodeList() {
        list = new ArrayList<>();
    }

    public NFBuildGraphNode get(int ordinal) {
        return list.get(ordinal);
    }

    boolean add(NFBuildGraphNode node) {
        return list.add(node);
    }

    public int size() {
        return list.size();
    }

    NFBuildGraphNode set(int ordinal, NFBuildGraphNode node) {
        return list.set(ordinal, node);
    }
}
