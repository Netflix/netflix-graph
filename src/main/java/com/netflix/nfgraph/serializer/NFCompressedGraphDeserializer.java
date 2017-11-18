/*
 *  Copyright 2013-2017 Netflix, Inc.
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

import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteData;
import com.netflix.nfgraph.util.ByteSegmentPool;
import com.netflix.nfgraph.util.SegmentedByteArray;
import com.netflix.nfgraph.util.SimpleByteArray;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * This class is used by {@link NFCompressedGraph#readFrom(InputStream)}.<p>
 *
 * It is unlikely that this class will need to be used externally.
 */
public class NFCompressedGraphDeserializer {

    private final NFCompressedGraphPointersDeserializer pointersDeserializer = new NFCompressedGraphPointersDeserializer();

    public NFCompressedGraph deserialize(InputStream is) throws IOException {
        return deserialize(is, null);
    }
    
    public NFCompressedGraph deserialize(InputStream is, ByteSegmentPool byteSegmentPool) throws IOException {
        DataInputStream dis = new DataInputStream(is);

        NFGraphSpec spec = deserializeSpec(dis);
        NFGraphModelHolder models = deserializeModels(dis);
        NFCompressedGraphPointers pointers = pointersDeserializer.deserializePointers(dis);
        long dataLength = deserializeDataLength(dis);
        ByteData data = deserializeData(dis, dataLength, byteSegmentPool);

        return new NFCompressedGraph(spec, models, data, dataLength, pointers);
    }


    private NFGraphSpec deserializeSpec(DataInputStream dis) throws IOException {
        int numNodes = dis.readInt();
        NFNodeSpec nodeSpecs[] = new NFNodeSpec[numNodes];

        for(int i=0;i<numNodes;i++) {
            String nodeTypeName = dis.readUTF();
            int numProperties = dis.readInt();
            NFPropertySpec propertySpecs[] = new NFPropertySpec[numProperties];

            for(int j=0;j<numProperties;j++) {
                String propertyName = dis.readUTF();
                String toNodeType = dis.readUTF();
                boolean isGlobal = dis.readBoolean();
                boolean isMultiple = dis.readBoolean();
                boolean isHashed = dis.readBoolean();

                propertySpecs[j] = new NFPropertySpec(propertyName, toNodeType, isGlobal, isMultiple, isHashed);
            }

            nodeSpecs[i] = new NFNodeSpec(nodeTypeName, propertySpecs);
        }

        return new NFGraphSpec(nodeSpecs);
    }

    private NFGraphModelHolder deserializeModels(DataInputStream dis) throws IOException {
        int numModels = dis.readInt();
        NFGraphModelHolder modelHolder = new NFGraphModelHolder();

        for(int i=0;i<numModels;i++) {
            modelHolder.getModelIndex(dis.readUTF());
        }

        return modelHolder;
    }

    /// Backwards compatibility:  If the data length is greater than Integer.MAX_VALUE, then
    /// -1 is serialized as an int before a long containing the actual length.
    private long deserializeDataLength(DataInputStream dis) throws IOException {
        int dataLength = dis.readInt();
        if(dataLength == -1) {
            return dis.readLong();
        }
        return dataLength;
    }

    private ByteData deserializeData(DataInputStream dis, long dataLength, ByteSegmentPool memoryPool) throws IOException {
        if(dataLength >= 0x20000000 || memoryPool != null) {
            SegmentedByteArray data = memoryPool == null ? new SegmentedByteArray(14) : new SegmentedByteArray(memoryPool);
            data.readFrom(dis, dataLength);
            return data;
        } else {
            byte data[] = new byte[(int)dataLength];
            dis.readFully(data);
            return new SimpleByteArray(data);
        }
    }
}