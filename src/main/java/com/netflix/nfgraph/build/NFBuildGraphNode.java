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

package com.netflix.nfgraph.build;

import java.util.Arrays;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

public class NFBuildGraphNode {

	private final NFNodeSpec nodeSpec;
	private NFBuildGraphNodeConnections[] connectionModelSpecificConnections;
	private final int ordinal;
	private int numIncomingConnections;
    
    NFBuildGraphNode(NFNodeSpec nodeSpec, int ordinal, int numKnownConnectionModels) {
    	this.nodeSpec = nodeSpec;
        this.connectionModelSpecificConnections = new NFBuildGraphNodeConnections[numKnownConnectionModels];
        this.ordinal = ordinal;
        this.numIncomingConnections = 0;
    }
    
    public int getOrdinal() {
    	return ordinal;
    }
    
    public int getConnection(int connectionModelIndex, NFPropertySpec spec) {
        NFBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if(connections == null)
            return -1;
        return connections.getConnection(spec);
    }
    
    public OrdinalSet getConnectionSet(int connectionModelIndex, NFPropertySpec spec) {
        NFBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if(connections == null)
            return OrdinalSet.EMPTY_SET;
        return connections.getConnectionSet(spec);
    }
    
    public OrdinalIterator getConnectionIterator(int connectionModelIndex, NFPropertySpec spec) {
        NFBuildGraphNodeConnections connections = getConnections(connectionModelIndex);
        if(connections == null)
            return OrdinalIterator.EMPTY_ITERATOR;
        return connections.getConnectionIterator(spec);
    }
    
    void addConnection(int connectionModelIndex, NFPropertySpec spec, int ordinal) {
    	NFBuildGraphNodeConnections connections = getOrCreateConnections(connectionModelIndex);
    	connections.addConnection(spec, ordinal);
    }
    
    void incrementNumIncomingConnections() {
    	numIncomingConnections++;
    }
    
    int numIncomingConnections() {
    	return numIncomingConnections;
    }
    
    private NFBuildGraphNodeConnections getConnections(int connectionModelIndex) {
    	if(connectionModelSpecificConnections.length <= connectionModelIndex)
    		return null;
        return connectionModelSpecificConnections[connectionModelIndex];
    }
    
    private NFBuildGraphNodeConnections getOrCreateConnections(int connectionModelIndex) {
    	if(connectionModelSpecificConnections.length <= connectionModelIndex)
    		connectionModelSpecificConnections = Arrays.copyOf(connectionModelSpecificConnections, connectionModelIndex + 1);
    	
    	if(connectionModelSpecificConnections[connectionModelIndex] == null) {
    		connectionModelSpecificConnections[connectionModelIndex] = new NFBuildGraphNodeConnections(nodeSpec);
    	}

        return connectionModelSpecificConnections[connectionModelIndex];
    }
    
}
