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
package com.netflix.nfgraph.serializer;

import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;
import com.netflix.nfgraph.util.ByteArrayBuffer;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class NFCompressedGraphPointersSerializer {

    private final NFCompressedGraphPointers pointers;
    private final long dataLength;

    NFCompressedGraphPointersSerializer(NFCompressedGraphPointers pointers, long dataLength) {
        this.pointers = pointers;
        this.dataLength = dataLength;
    }

    void serializePointers(DataOutputStream dos) throws IOException {
        int numNodeTypes = pointers.asMap().size();
        if(dataLength > 0xFFFFFFFFL)
            numNodeTypes |= Integer.MIN_VALUE;

        /// In order to maintain backwards compatibility of produced artifacts,
        /// if more than 32 bits is required to represent the pointers, then flag
        /// the sign bit in the serialized number of node types.
        dos.writeInt(numNodeTypes);

        for(Map.Entry<String, long[]>entry : pointers.asMap().entrySet()) {
            dos.writeUTF(entry.getKey());
            serializePointerArray(dos, entry.getValue());
        }
    }

    private void serializePointerArray(DataOutputStream dos, long pointers[]) throws IOException {
        ByteArrayBuffer buf = new ByteArrayBuffer();

        long currentPointer = 0;

        for(int i=0;i<pointers.length;i++) {
            if(pointers[i] == -1) {
                buf.writeVInt(-1);
            } else {
                buf.writeVInt((int)(pointers[i] - currentPointer));
                currentPointer = pointers[i];
            }
        }

        dos.writeInt(pointers.length);
        dos.writeInt((int)buf.length());
        buf.copyTo(dos);
    }

}
