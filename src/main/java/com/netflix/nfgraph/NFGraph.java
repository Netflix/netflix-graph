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

package com.netflix.nfgraph;

import com.netflix.nfgraph.build.NFBuildGraph;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.util.OrdinalMap;

/**
 * <code>NFGraph</code> represents a directed graph and is the base class for the two flavors of NetflixGraph 
 * ({@link NFBuildGraph} and {@link NFCompressedGraph}).  It defines the operations for retrieving connections 
 * in the graph, given some node and property.<p>
 * 
 * In the NetflixGraph library, each node in your graph is expected to be uniquely represented as a "type" and "ordinal".  
 * Each "type" will be referred to by some String.  An "ordinal", in this sense, is a number that uniquely defines the node 
 * given its type.  If a type of node has "n" instances, then each instance should be representable by some unique value 
 * from 0 through (n-1).  If nodes in the graph are represented as Objects externally to the NetflixGraph library, then 
 * developers may find it helpful to use an {@link OrdinalMap} for each type to create and maintain a mapping between objects 
 * and their ordinals.  The {@link OrdinalMap} has been optimized with this use case in mind.  <p>
 * 
 * Use of the NFGraph is expected to generally follow some lifecycle:<p>
 * <ol>
 * <li>Define an {@link NFGraphSpec}, which serves as the schema for the graph data.</li>
 * <li>Instantiate an {@link NFBuildGraph}, then populate it with connections.</li>
 * <li>Compress the {@link NFBuildGraph}, which will return a representation of the data as an {@link NFCompressedGraph}.</li>
 * <li>Serialize the {@link NFCompressedGraph} to a stream.  Netflix, for example, has a use case which streams this graph to Amazon Web Service's S3.</li>
 * <li>Deserialize the stream where the compact in-memory representation of the graph data is necessary.</li>
 * </ol><p>
 * 
 * In some cases, the location where the compact in-memory representation is necessary is the same as the location where this
 * representation will be built.  In these cases, steps (4) and (5) above will be omitted.<p>
 * 
 * If there will be a producer of this graph and one or more consumers, then your producer code will resemble:<p>
 * 
 * <pre>
 * {@code
 * NFGraphSpec spec = new NFGraphSpec( ... );
 * 
 * NFBuildGraph buildGraph = new NFBuildGraph(spec);
 * 
 * for( ... each connection between nodes ... ) {
 *     graph.addConnection( ... );
 * }
 * 
 * NFCompressedGraph compressedGraph = buildGraph.compress();
 * 
 * OutputStream os = ... stream to where you want the serialized data ...;
 * 
 * compressedGraph.writeTo(os);
 * }
 * </pre>
 * 
 * And your consumer code will resemble:<p>
 * 
 * <pre>
 * {@code
 * InputStream is = ... stream from where the serialized data was written ...;
 * 
 * NFGraph graph = NFCompressedGraph.readFrom(is);
 * }
 * </pre>
 *
 * @see NFGraphSpec
 * @see NFBuildGraph
 * @see NFCompressedGraph
 *
 * @author dkoszewnik
 */
public abstract class NFGraph {
	
    protected final NFGraphSpec graphSpec;
    protected final NFGraphModelHolder modelHolder;


    protected NFGraph(NFGraphSpec graphSpec) {
        this.graphSpec = graphSpec;
        this.modelHolder = new NFGraphModelHolder();
    }
    
    protected NFGraph(NFGraphSpec graphSpec, NFGraphModelHolder modelHolder) {
    	this.graphSpec = graphSpec;
    	this.modelHolder = modelHolder;
    }

    /**
     * Retrieve a single connected ordinal, given the type and ordinal of the originating node, and the property by which this node is connected.
     * 
     * @return the connected ordinal, or -1 if there is no such ordinal
     */
    public int getConnection(String nodeType, int ordinal, String propertyName) {
        return getConnection(0, nodeType, ordinal, propertyName);
    }

    /**
     * Retrieve a single connected ordinal in a given connection model, given the type and ordinal of the originating node, and the property by which this node is connected.
     * 
     * @return the connected ordinal, or -1 if there is no such ordinal
     */
    public int getConnection(String connectionModel, String nodeType, int ordinal, String propertyName) {
    	int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        return getConnection(connectionModelIndex, nodeType, ordinal, propertyName);
    }
    
    /**
     * Retrieve an {@link OrdinalIterator} over all connected ordinals, given the type and ordinal of the originating node, and the property by which this node is connected.
     * 
     * @return an {@link OrdinalIterator} over all connected ordinals
     */
    public OrdinalIterator getConnectionIterator(String nodeType, int ordinal, String propertyName) {
        return getConnectionIterator(0, nodeType, ordinal, propertyName);
    }

    /**
     * Retrieve an {@link OrdinalIterator} over all connected ordinals in a given connection model, given the type and ordinal of the originating node, and the property by which this node is connected.
     * 
     * @return an {@link OrdinalIterator} over all connected ordinals
     */
    public OrdinalIterator getConnectionIterator(String connectionModel, String nodeType, int ordinal, String propertyName) {
    	int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        return getConnectionIterator(connectionModelIndex, nodeType, ordinal, propertyName);
    }

    /**
     * Retrieve an {@link OrdinalSet} over all connected ordinals, given the type and ordinal of the originating node, and the property by which this node is connected.
     * 
     * @return an {@link OrdinalSet} over all connected ordinals
     */
    public OrdinalSet getConnectionSet(String nodeType, int ordinal, String propertyName) {
        return getConnectionSet(0, nodeType, ordinal, propertyName);
    }
    
    /**
     * Retrieve an {@link OrdinalSet} over all connected ordinals in a given connection model, given the type and ordinal of the originating node, and the property by which this node is connected.
     * 
     * @return an {@link OrdinalSet} over all connected ordinals
     */
    public OrdinalSet getConnectionSet(String connectionModel, String nodeType, int ordinal, String propertyName) {
    	int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        return getConnectionSet(connectionModelIndex, nodeType, ordinal, propertyName);
    }
    
    protected abstract int getConnection(int connectionModelIndex, String nodeType, int ordinal, String propertyName);

    protected abstract OrdinalSet getConnectionSet(int connectionModelIndex, String nodeType, int ordinal, String propertyName);

    protected abstract OrdinalIterator getConnectionIterator(int connectionModelIndex, String nodeType, int ordinal, String propertyName);

}
