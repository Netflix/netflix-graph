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

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayBuffer;

/**
 * This class is used by {@link NFCompressedGraph#writeTo(OutputStream)}.<p/>
 * 
 * It is unlikely that this class will need to be used externally.
 */
public class NFCompressedGraphSerializer {

    private final NFGraphSpec spec;
    private final NFGraphModelHolder modelHolder;
    private final NFCompressedGraphPointers pointers;
    private final byte data[];
    
    public NFCompressedGraphSerializer(NFGraphSpec spec, NFGraphModelHolder modelHolder, NFCompressedGraphPointers pointers, byte data[]) {
        this.spec = spec;
        this.modelHolder = modelHolder;
        this.pointers = pointers;
        this.data = data;
    }

    public void serializeTo(OutputStream os) throws IOException {
        DataOutputStream dos = new DataOutputStream(os);

        serializeSpec(dos);
        serializeModels(dos);
        serializePointers(dos);
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

    private void serializePointers(DataOutputStream dos) throws IOException {
        dos.writeInt(pointers.asMap().size());
        
        for(Map.Entry<String, int[]>entry : pointers.asMap().entrySet()) {
            dos.writeUTF(entry.getKey());
            serializePointerArray(dos, entry.getValue());
        }
    }
    
    private void serializePointerArray(DataOutputStream dos, int pointers[]) throws IOException {
        ByteArrayBuffer buf = new ByteArrayBuffer();
        
        int currentPointer = 0;
        
        for(int i=0;i<pointers.length;i++) {
            if(pointers[i] == -1) {
                buf.writeVInt(-1);
            } else {
                buf.writeVInt(pointers[i] - currentPointer);
                currentPointer = pointers[i];
            }
        }
        
        dos.writeInt(pointers.length);
        dos.writeInt(buf.length());
        buf.copyTo(dos);
    }
    
    private void serializeData(DataOutputStream dos) throws IOException {
        dos.writeInt(data.length);
        dos.write(data);
    }
}