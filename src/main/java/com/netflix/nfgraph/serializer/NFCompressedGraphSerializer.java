/*
 *  Copyright 2013 Netflix, Inc.
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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * This class is used by {@link NFCompressedGraph#writeTo(OutputStream)}.<p>
 *
 * It is unlikely that this class will need to be used externally.
 */
public class NFCompressedGraphSerializer {

    private final NFGraphSpec spec;
    private final NFGraphModelHolder modelHolder;
    private final NFCompressedGraphPointersSerializer pointersSerializer;
    private final ByteData data;
    private final long dataLength;

    public NFCompressedGraphSerializer(NFGraphSpec spec, NFGraphModelHolder modelHolder, NFCompressedGraphPointers pointers, ByteData data, long dataLength) {
        this.spec = spec;
        this.modelHolder = modelHolder;
        this.pointersSerializer = new NFCompressedGraphPointersSerializer(pointers, dataLength);
        this.data = data;
        this.dataLength = dataLength;
    }

    public void serializeTo(OutputStream os) throws IOException {
        DataOutputStream dos = new DataOutputStream(os);

        serializeSpec(dos);
        serializeModels(dos);
        pointersSerializer.serializePointers(dos);
        serializeData(dos);

        dos.flush();
    }

    private void serializeSpec(DataOutputStream dos) throws IOException {
        dos.writeInt(spec.size());

        for(NFNodeSpec nodeSpec : spec) {
            dos.writeUTF(nodeSpec.getNodeTypeName());
            dos.writeInt(nodeSpec.getPropertySpecs().length);

            for(NFPropertySpec propertySpec : nodeSpec.getPropertySpecs()) {
                dos.writeUTF(propertySpec.getName());
                dos.writeUTF(propertySpec.getToNodeType());
                dos.writeBoolean(propertySpec.isGlobal());
                dos.writeBoolean(propertySpec.isMultiple());
                dos.writeBoolean(propertySpec.isHashed());
            }
        }
    }

    private void serializeModels(DataOutputStream dos) throws IOException {
        dos.writeInt(modelHolder.size());
        for(String model : modelHolder) {
            dos.writeUTF(model);
        }
    }

    private void serializeData(DataOutputStream dos) throws IOException {
        /// In order to maintain backwards compatibility of produced artifacts,
        /// if more than Integer.MAX_VALUE bytes are required in the data,
        /// first serialize a negative 1 integer, then serialize the number
        /// of required bits as a long.
        if(dataLength > Integer.MAX_VALUE) {
            dos.writeInt(-1);
            dos.writeLong(dataLength);
        } else {
            dos.writeInt((int)dataLength);
        }

        data.writeTo(dos, dataLength);
    }
}