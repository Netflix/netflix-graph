package com.netflix.nfgraph.compressed;

import java.util.Map;

public interface NFCompressedGraphPointers {

    /**
     * @return the offset into the {@link NFCompressedGraph}'s byte array for the node identified by the given type and ordinal.
     */
    public long getPointer(String nodeType, int ordinal);

    public int numPointers(String nodeType);

    public Map<String, long[]> asMap();

}
