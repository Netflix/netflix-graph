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

import static com.netflix.nfgraph.NFGraphModelHolder.CONNECTION_MODEL_GLOBAL;

import com.netflix.nfgraph.NFGraph;
import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.compressor.NFCompressedGraphBuilder;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

/**
 * An </code>NFBuildGraph</code> is used to create a new graph.  This representation of the graph data is not especially memory-efficient,
 * and is intended to exist only for a short time while the {@link NFGraph} is being populated.<p/>
 * 
 * Once the graph is completely populated, it is expected that this <code>NFBuildGraph</code> will be exchanged for a memory-efficient, 
 * read-only {@link NFCompressedGraph} via the <code>compress()</code> method.<p/>
 * 
 * See {@link NFGraph} for an example of code which creates and populates an <code>NFBuildGraph</code>
 * 
 *
 */
public class NFBuildGraph extends NFGraph {

    private final NFBuildGraphNodeCache nodeCache;
    
    public NFBuildGraph(NFGraphSpec graphSpec) {
        super(graphSpec);
        this.nodeCache = new NFBuildGraphNodeCache(graphSpec, modelHolder);
    }

    @Override
	protected int getConnection(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
		NFBuildGraphNode node = nodeCache.getNode(nodeType, ordinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, propertyName);
		return node.getConnection(connectionModelIndex, propertySpec);
	}

    @Override
    protected OrdinalIterator getConnectionIterator(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
		NFBuildGraphNode node = nodeCache.getNode(nodeType, ordinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, propertyName);
        return node.getConnectionIterator(connectionModelIndex, propertySpec);
	}

    @Override
    protected OrdinalSet getConnectionSet(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
		NFBuildGraphNode node = nodeCache.getNode(nodeType, ordinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, propertyName);
        return node.getConnectionSet(connectionModelIndex, propertySpec);
    }

    /**
     * Add a connection to this graph.  The connection will be from the node identified by the given <code>nodeType</code> and <code>fromOrdinal</code>.
     * The connection will be via the specified <code>viaProperty</code> in the {@link NFNodeSpec} for the given <code>nodeType</code>.
     * The connection will be to the node identified by the given <code>toOrdinal</code>.  The type of the to node is implied by the <code>viaProperty</code>.
     */
    public void addConnection(String nodeType, int fromOrdinal, String viaProperty, int toOrdinal) {
        addConnection(CONNECTION_MODEL_GLOBAL, nodeType, fromOrdinal, viaProperty, toOrdinal);
    }
    
    /**
     * Add a connection to this graph.  The connection will be in the given connection model.  The connection will be from the node identified by the given 
     * <code>nodeType</code> and <code>fromOrdinal</code>. The connection will be via the specified <code>viaProperty</code> in the {@link NFNodeSpec} for 
     * the given <code>nodeType</code>. The connection will be to the node identified by the given <code>toOrdinal</code>.  The type of the to node is implied 
     * by the <code>viaProperty</code>.
     */
    public void addConnection(String connectionModel, String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal) {
        NFBuildGraphNode node = nodeCache.getNode(nodeType, fromOrdinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, viaPropertyName);
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        node.addConnection(connectionModelIndex, propertySpec, toOrdinal);
        
        NFBuildGraphNode toNode = nodeCache.getNode(propertySpec.getToNodeType(), toOrdinal);
        toNode.incrementNumIncomingConnections();
    }
    
    /**
     * Add a connection model, identified by the parameter <code>connectionModel</code> to this graph.<p/>
     * 
     * Building the graph may be much more efficient if each connection model is added to the graph with this method 
     * prior to adding any connections.<p/>
     * 
     * This operation is not necessary, but may make building the graph more efficient.
     */
    public void addConnectionModel(String connectionModel) {
    	modelHolder.getModelIndex(connectionModel);
    }
    
    
    private NFPropertySpec getPropertySpec(String nodeType, String propertyName) {
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);
        NFPropertySpec propertySpec = nodeSpec.getPropertySpec(propertyName);
        return propertySpec;
    }

    /**
     * Return a {@link NFCompressedGraph} containing all connections which have been added to this <code>NFBuildGraph</code>.
     */
    public NFCompressedGraph compress() {
        NFCompressedGraphBuilder builder = new NFCompressedGraphBuilder(graphSpec, nodeCache, modelHolder);
        return builder.buildGraph();
    }
}