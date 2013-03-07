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

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayReader;

/**
 * This class is used by {@link NFCompressedGraph#readFrom(InputStream)}.<p/>
 * 
 * It is unlikely that this class will need to be used externally.
 */
public class NFCompressedGraphDeserializer {

    public NFCompressedGraph deserialize(InputStream is) throws IOException {
        DataInputStream dis = new DataInputStream(is);
        
        NFGraphSpec spec = deserializeSpec(dis);
        NFGraphModelHolder models = deserializeModels(dis);
        NFCompressedGraphPointers pointers = deserializePointers(dis);
        byte data[] = deserializeData(dis);
        
        return new NFCompressedGraph(spec, models, data, pointers);
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

    private NFCompressedGraphPointers deserializePointers(DataInputStream dis) throws IOException {
        int numTypes = dis.readInt();
        NFCompressedGraphPointers pointers = new NFCompressedGraphPointers();
        
        for(int i=0;i<numTypes;i++) {
            String nodeType = dis.readUTF();
            
            pointers.addPointers(nodeType, deserializePointerArray(dis));
        }
        
        return pointers;
    }

    private int[] deserializePointerArray(DataInputStream dis) throws IOException {
        int numNodes = dis.readInt();
        int numBytes = dis.readInt();
        
        byte data[] = new byte[numBytes];
        int pointers[] = new int[numNodes];

        dis.readFully(data);
        
        ByteArrayReader reader = new ByteArrayReader(data, 0);
        
        int currentPointer = 0;
        
        for(int i=0;i<numNodes;i++) {
            int vInt = reader.readVInt();
            if(vInt == -1) {
                pointers[i] = -1;
            } else {
                currentPointer += vInt;
                pointers[i] = currentPointer;
            }
        }
        
        return pointers;
    }

    private byte[] deserializeData(DataInputStream dis) throws IOException {
        int dataLength = dis.readInt();
        byte data[] = new byte[dataLength];
        dis.readFully(data);
        return data;
    }
}