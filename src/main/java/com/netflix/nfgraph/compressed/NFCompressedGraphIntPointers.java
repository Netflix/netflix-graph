/*
 *  Copyright 2014 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */

package com.netflix.nfgraph.compressed;

import java.util.HashMap;
import java.util.Map;

import com.netflix.nfgraph.exception.NFGraphException;

/**
 * This class holds all of the offsets into the {@link NFCompressedGraph}'s byte array.<p>
 *
 * This class maintains a mapping of type name to int array.  For a given type, the offset in the {@link NFCompressedGraph}'s byte array
 * where the connections for a given node are encoded is equal to the value of the int array for the node's type at the index for the node's ordinal.<p>
 *
 * It is unlikely that this class will need to be used externally.
 */
public class NFCompressedGraphIntPointers implements NFCompressedGraphPointers {

    private final Map<String, int[]>pointersByOrdinal;

    public NFCompressedGraphIntPointers() {
        this.pointersByOrdinal = new HashMap<String, int[]>();
    }

    /**
     * @return the offset into the {@link NFCompressedGraph}'s byte array for the node identified by the given type and ordinal.
     */
    public long getPointer(String nodeType, int ordinal) {
        int pointers[] = pointersByOrdinal.get(nodeType);
        if(pointers == null)
            throw new NFGraphException("Undefined node type: " + nodeType);
        if(ordinal < pointers.length) {
            if(pointers[ordinal] == -1)
                return -1;
            return 0xFFFFFFFFL & pointers[ordinal];
        }
        return -1;
    }

    public void addPointers(String nodeType, int pointers[]) {
        pointersByOrdinal.put(nodeType, pointers);
    }

    public int numPointers(String nodeType) {
        return pointersByOrdinal.get(nodeType).length;
    }

    @Override
    public Map<String, long[]> asMap() {
        Map<String, long[]> map = new HashMap<String, long[]>();

        for(Map.Entry<String, int[]> entry : pointersByOrdinal.entrySet()) {
            map.put(entry.getKey(), toLongArray(entry.getValue()));
        }

        return map;
    }

    private long[] toLongArray(int[] arr) {
        long l[] = new long[arr.length];

        for(int i=0;i<arr.length;i++) {
            l[i] = arr[i];
        }

        return l;
    }

}
