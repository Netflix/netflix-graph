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

package com.netflix.nfgraph.compressor;

import java.util.List;

import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.build.NFBuildGraph;
import com.netflix.nfgraph.build.NFBuildGraphNode;
import com.netflix.nfgraph.build.NFBuildGraphNodeCache;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayBuffer;

/**
 * <code>NFCompressedGraphBuilder</code> is used by {@link NFBuildGraph#compress()} to create an {@link NFCompressedGraph}.<p/>
 * 
 * It is unlikely that this class will need to be used externally.
 */
public class NFCompressedGraphBuilder {

    private final NFGraphSpec graphSpec;
    private final NFBuildGraphNodeCache buildGraphNodeCache;
    private final NFGraphModelHolder modelHolder;
    
    private final ByteArrayBuffer graphBuffer;
    private final ByteArrayBuffer modelBuffer;
    private final ByteArrayBuffer fieldBuffer;
    
    private final CompactPropertyBuilder compactPropertyBuilder;
    private final HashedPropertyBuilder hashedPropertyBuilder;
    private final BitSetPropertyBuilder bitSetPropertyBuilder;
    
    private NFCompressedGraphPointers compressedGraphPointers;
    
    public NFCompressedGraphBuilder(NFGraphSpec graphSpec, NFBuildGraphNodeCache buildGraphNodeCache, NFGraphModelHolder modelHolder) {
        this.graphSpec = graphSpec;
        this.buildGraphNodeCache = buildGraphNodeCache;
        this.modelHolder = modelHolder;
        
        this.graphBuffer = new ByteArrayBuffer();
        this.modelBuffer = new ByteArrayBuffer();
        this.fieldBuffer = new ByteArrayBuffer();
        
        this.compactPropertyBuilder = new CompactPropertyBuilder(fieldBuffer);
        this.hashedPropertyBuilder = new HashedPropertyBuilder(fieldBuffer);
        this.bitSetPropertyBuilder = new BitSetPropertyBuilder(fieldBuffer);
        
        this.compressedGraphPointers = new NFCompressedGraphPointers();
    }
    
    public NFCompressedGraph buildGraph() {
    	for(String nodeType : graphSpec.getNodeTypes()) {
    		List<NFBuildGraphNode> nodeOrdinals = buildGraphNodeCache.getNodes(nodeType);
    		addNodeType(nodeType, nodeOrdinals);
    	}
        
        return new NFCompressedGraph(graphSpec, modelHolder, graphBuffer.getData(), compressedGraphPointers);
    }

    private void addNodeType(String nodeType, List<NFBuildGraphNode> nodes) {
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);
        int ordinalPointers[] = new int[nodes.size()];
        
        for(int i=0;i<nodes.size();i++) {
            NFBuildGraphNode node = nodes.get(i);
            if(node != null) {
                ordinalPointers[i] = graphBuffer.length();
                serializeNode(node, nodeSpec);
            } else {
                ordinalPointers[i] = -1;
            }
        }
        
        compressedGraphPointers.addPointers(nodeType, ordinalPointers);
    }
    
    private void serializeNode(NFBuildGraphNode node, NFNodeSpec nodeSpec) {
        for(NFPropertySpec propertySpec : nodeSpec.getPropertySpecs()) {
            serializeProperty(node, propertySpec);
        }
    }
    
    private void serializeProperty(NFBuildGraphNode node, NFPropertySpec propertySpec) {
        if(propertySpec.isConnectionModelSpecific()) {
            for(int i=0;i<modelHolder.size();i++) {
                serializeProperty(node, propertySpec, i, modelBuffer);
            }
            copyBuffer(modelBuffer, graphBuffer);
        } else {
            serializeProperty(node, propertySpec, 0, graphBuffer);
        }
    }
    
    private void serializeProperty(NFBuildGraphNode node, NFPropertySpec propertySpec, int connectionModelIndex, ByteArrayBuffer toBuffer) {
        if(propertySpec.isMultiple()) {
            serializeMultipleProperty(node, propertySpec, connectionModelIndex, toBuffer);
        } else {
            int connection = node.getConnection(connectionModelIndex, propertySpec);
            if(connection == -1) {
                toBuffer.writeByte((byte)0x80);
            } else {
                toBuffer.writeVInt(connection);
            }
        }
    }

    private void serializeMultipleProperty(NFBuildGraphNode node, NFPropertySpec propertySpec, int connectionModelIndex, ByteArrayBuffer toBuffer) {
        OrdinalSet connections = node.getConnectionSet(connectionModelIndex, propertySpec);
        
        int numBitsInBitSet = buildGraphNodeCache.numNodes(propertySpec.getToNodeType());
		int bitSetSize = ((numBitsInBitSet - 1) / 8) + 1;
        
        if(connections.size() < bitSetSize) {
        	if(propertySpec.isHashed()) {
        		hashedPropertyBuilder.buildProperty(connections);
        		if(fieldBuffer.length() < bitSetSize) {
        	        int log2BytesUsed = 32 - Integer.numberOfLeadingZeros(fieldBuffer.length());
        	        toBuffer.writeByte((byte)log2BytesUsed);
        			toBuffer.write(fieldBuffer);
        			fieldBuffer.reset();
        			return;
        		}
        	} else {
        		compactPropertyBuilder.buildProperty(connections);
        		if(fieldBuffer.length() < bitSetSize) {
        			toBuffer.writeVInt(fieldBuffer.length());
        			toBuffer.write(fieldBuffer);
        			fieldBuffer.reset();
        			return;
        		}
        	}
        	
        	fieldBuffer.reset();
        }
        
        bitSetPropertyBuilder.buildProperty(connections, numBitsInBitSet);
        toBuffer.writeByte((byte)0x80);
        toBuffer.write(fieldBuffer);
        fieldBuffer.reset();
    }
    
    private void copyBuffer(ByteArrayBuffer from, ByteArrayBuffer to) {
        to.writeVInt(from.length());
        to.write(from);
        from.reset();
    }
    
}