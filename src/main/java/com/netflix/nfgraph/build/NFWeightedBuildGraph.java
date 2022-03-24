package com.netflix.nfgraph.build;

import com.netflix.nfgraph.NFGraph;
import com.netflix.nfgraph.WeightedOrdinalIterator;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.compressed.NFCompressedGraph;
import com.netflix.nfgraph.compressed.NFWeightedCompressedGraph;
import com.netflix.nfgraph.compressor.NFWeightedCompressedGraphBuilder;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;

import static com.netflix.nfgraph.NFGraphModelHolder.CONNECTION_MODEL_GLOBAL;

public class NFWeightedBuildGraph extends NFGraph {

    private final NFWeightedBuildGraphNodeCache nodeCache;

    public NFWeightedBuildGraph(NFGraphSpec graphSpec) {
        super(graphSpec);
        this.nodeCache = new NFWeightedBuildGraphNodeCache(graphSpec, modelHolder);
    }

    /**
     * Retrieve an {@link WeightedOrdinalIterator} over all connected ordinals, given the type and ordinal of the originating node, and the property by which this node is connected.
     *
     * @return an {@link WeightedOrdinalIterator} over all connected ordinals
     */
    public WeightedOrdinalIterator getWeightedConnectionIterator(String nodeType, int ordinal, String propertyName) {
        return getConnectionIterator(0, nodeType, ordinal, propertyName);
    }

    /**
     * Retrieve an {@link WeightedOrdinalIterator} over all connected ordinals in a given connection model, given the type and ordinal of the originating node, and the property by which this node is connected.
     *
     * @return an {@link WeightedOrdinalIterator} over all connected ordinals
     */
    public WeightedOrdinalIterator getWeightedConnectionIterator(String connectionModel, String nodeType, int ordinal, String propertyName) {
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        return getConnectionIterator(connectionModelIndex, nodeType, ordinal, propertyName);
    }

    /**
     * Retrieve an {@link WeightedOrdinalSet} over all connected ordinals, given the type and ordinal of the originating node, and the property by which this node is connected.
     *
     * @return an {@link WeightedOrdinalSet} over all connected ordinals
     */
    public WeightedOrdinalSet getWeightedConnectionSet(String nodeType, int ordinal, String propertyName) {
        return getConnectionSet(0, nodeType, ordinal, propertyName);
    }

    /**
     * Retrieve an {@link WeightedOrdinalSet} over all connected ordinals in a given connection model, given the type and ordinal of the originating node, and the property by which this node is connected.
     *
     * @return an {@link WeightedOrdinalSet} over all connected ordinals
     */
    public WeightedOrdinalSet getWeightedConnectionSet(String connectionModel, String nodeType, int ordinal, String propertyName) {
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        return getConnectionSet(connectionModelIndex, nodeType, ordinal, propertyName);
    }

    @Override
    protected int getConnection(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        NFWeightedBuildGraphNode node = nodeCache.getNode(nodeType, ordinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, propertyName);
        return node.getConnection(connectionModelIndex, propertySpec);
    }

    @Override
    protected WeightedOrdinalIterator getConnectionIterator(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        NFWeightedBuildGraphNode node = nodeCache.getNode(nodeType, ordinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, propertyName);
        return node.getConnectionIterator(connectionModelIndex, propertySpec);
    }

    @Override
    protected WeightedOrdinalSet getConnectionSet(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        NFWeightedBuildGraphNode node = nodeCache.getNode(nodeType, ordinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, propertyName);
        return node.getConnectionSet(connectionModelIndex, propertySpec);
    }

    /**
     * Add a connection to this graph.  The connection will be from the node identified by the given <code>nodeType</code> and <code>fromOrdinal</code>.
     * The connection will be via the specified <code>viaProperty</code> in the {@link NFNodeSpec} for the given <code>nodeType</code>.
     * The connection will be to the node identified by the given <code>toOrdinal</code>.  The type of the to node is implied by the <code>viaProperty</code>.
     */
    public void addConnection(String nodeType, int fromOrdinal, String viaProperty, int toOrdinal, int weight, int property) {
        addConnection(CONNECTION_MODEL_GLOBAL, nodeType, fromOrdinal, viaProperty, toOrdinal, weight, property);
    }

    /**
     * Add a connection to this graph.  The connection will be in the given connection model.  The connection will be from the node identified by the given
     * <code>nodeType</code> and <code>fromOrdinal</code>. The connection will be via the specified <code>viaProperty</code> in the {@link NFNodeSpec} for
     * the given <code>nodeType</code>. The connection will be to the node identified by the given <code>toOrdinal</code>.  The type of the to node is implied
     * by the <code>viaProperty</code>.
     */
    public void addConnection(String connectionModel, String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal, int weight, int property) {
        if (property == WeightedOrdinalIterator.INVALID_PROPERTY) {
            throw new IllegalArgumentException(String.format("Invalid property value %d", WeightedOrdinalIterator.INVALID_PROPERTY));
        }
        NFWeightedBuildGraphNode fromNode = nodeCache.getNode(nodeType, fromOrdinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, viaPropertyName);
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        NFWeightedBuildGraphNode toNode = nodeCache.getNode(propertySpec.getToNodeType(), toOrdinal);

        fromNode.addConnection(connectionModelIndex, propertySpec, toNode.getOrdinal(), weight, property);
        fromNode.incrementNumOutgoingConnections();
        toNode.incrementNumIncomingConnections();
    }

    /**
     * Add a connection to this graph.  The connection will be from the node identified by the given <code>nodeType</code> and <code>fromOrdinal</code>.
     * The connection will be via the specified <code>viaProperty</code> in the {@link NFNodeSpec} for the given <code>nodeType</code>.
     * The connection will be to the node identified by the given <code>toOrdinal</code>.  The type of the to node is implied by the <code>viaProperty</code>.
     */
    public void addConnection(String nodeType, int fromOrdinal, String viaProperty, int toOrdinal, int weight) {
        addConnection(CONNECTION_MODEL_GLOBAL, nodeType, fromOrdinal, viaProperty, toOrdinal, weight);
    }

    /**
     * Add a connection to this graph.  The connection will be in the given connection model.  The connection will be from the node identified by the given
     * <code>nodeType</code> and <code>fromOrdinal</code>. The connection will be via the specified <code>viaProperty</code> in the {@link NFNodeSpec} for
     * the given <code>nodeType</code>. The connection will be to the node identified by the given <code>toOrdinal</code>.  The type of the to node is implied
     * by the <code>viaProperty</code>.
     */
    public void addConnection(String connectionModel, String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal, int weight) {
        NFWeightedBuildGraphNode fromNode = nodeCache.getNode(nodeType, fromOrdinal);
        NFPropertySpec propertySpec = getPropertySpec(nodeType, viaPropertyName);
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        NFWeightedBuildGraphNode toNode = nodeCache.getNode(propertySpec.getToNodeType(), toOrdinal);

        fromNode.addConnection(connectionModelIndex, propertySpec, toNode.getOrdinal(), weight);
        fromNode.incrementNumOutgoingConnections();
        toNode.incrementNumIncomingConnections();
    }

    public int getConnectionWeight(String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal) {
        return getConnectionWeight(CONNECTION_MODEL_GLOBAL, nodeType, fromOrdinal, viaPropertyName, toOrdinal);
    }

    public int getConnectionWeight(String connectionModel, String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal) {
        NFPropertySpec propertySpec = getPropertySpec(nodeType, viaPropertyName);
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        NFWeightedBuildGraphNode fromNode = nodeCache.getNode(nodeType, fromOrdinal);
        NFWeightedBuildGraphNode toNode = nodeCache.getNode(propertySpec.getToNodeType(), toOrdinal);
        return fromNode.getConnectionWeight(connectionModelIndex, propertySpec, toNode.getOrdinal());
    }

    public int getConnectionProperty(String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal) {
        return getConnectionProperty(CONNECTION_MODEL_GLOBAL, nodeType, fromOrdinal, viaPropertyName, toOrdinal);
    }

    public int getConnectionProperty(String connectionModel, String nodeType, int fromOrdinal, String viaPropertyName, int toOrdinal) {
        NFPropertySpec propertySpec = getPropertySpec(nodeType, viaPropertyName);
        int connectionModelIndex = modelHolder.getModelIndex(connectionModel);
        NFWeightedBuildGraphNode fromNode = nodeCache.getNode(nodeType, fromOrdinal);
        NFWeightedBuildGraphNode toNode = nodeCache.getNode(propertySpec.getToNodeType(), toOrdinal);
        return fromNode.getConnectionProperty(connectionModelIndex, propertySpec, toNode.getOrdinal());
    }

    /**
     * Returns the list of {@link com.netflix.nfgraph.build.NFWeightedBuildGraphNode}s associated with the specified
     * <code>nodeType</code>.
     */
    public NFWeightedBuildGraphNodeList getNodes(String nodeType) {
        return nodeCache.getNodes(nodeType);
    }

    /**
     * Creates an {@link com.netflix.nfgraph.build.NFWeightedBuildGraphNode} for <code>nodeSpec</code> and <code>ordinal</code>
     * and adds it to <code>nodes</code>.  If such a node exists in <code>nodes</code>, then that node is returned.
     */
    public NFWeightedBuildGraphNode getOrCreateNode(NFWeightedBuildGraphNodeList nodes, NFNodeSpec nodeSpec, int ordinal) {
        return nodeCache.getNode(nodes, nodeSpec, ordinal);
    }

    /**
     * Add a connection model, identified by the parameter <code>connectionModel</code> to this graph.<p>
     * <p>
     * Building the graph may be much more efficient if each connection model is added to the graph with this method
     * prior to adding any connections.<p>
     * <p>
     * This operation is not necessary, but may make building the graph more efficient.
     * <p>
     * Returns the "model index" used to identify the connection model internally.  Passing this to
     * the various {@code addConnection()} may offer a performance boost while building the graph.
     */
    public int addConnectionModel(String connectionModel) {
        return modelHolder.getModelIndex(connectionModel);
    }

    /**
     * Returns the {@link NFPropertySpec} associated with the supplied node type and property name.
     */
    public NFPropertySpec getPropertySpec(String nodeType, String propertyName) {
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);
        NFPropertySpec propertySpec = nodeSpec.getPropertySpec(propertyName);
        return propertySpec;
    }

    /**
     * Return a {@link NFCompressedGraph} containing all connections which have been added to this <code>NFBuildGraph</code>.
     */
    public NFWeightedCompressedGraph compress() {
        NFWeightedCompressedGraphBuilder builder = new NFWeightedCompressedGraphBuilder(graphSpec, nodeCache, modelHolder);
        return builder.buildGraph();
    }
}
