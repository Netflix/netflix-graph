package com.netflix.nfgraph.compressor;

import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.WeightedOrdinalSet;
import com.netflix.nfgraph.build.NFWeightedBuildGraph;
import com.netflix.nfgraph.build.NFWeightedBuildGraphNode;
import com.netflix.nfgraph.build.NFWeightedBuildGraphNodeCache;
import com.netflix.nfgraph.build.NFWeightedBuildGraphNodeList;
import com.netflix.nfgraph.compressed.NFCompressedGraphLongPointers;
import com.netflix.nfgraph.compressed.NFWeightedCompressedGraph;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayBuffer;

/**
 * <code>NFCompressedGraphBuilder</code> is used by {@link NFWeightedBuildGraph#compress()} to create an {@link NFWeightedCompressedGraph}.<p>
 * <p>
 * It is unlikely that this class will need to be used externally.
 */
public class NFWeightedCompressedGraphBuilder {

    private final NFGraphSpec graphSpec;
    private final NFWeightedBuildGraphNodeCache buildGraphNodeCache;
    private final NFGraphModelHolder modelHolder;

    private final ByteArrayBuffer graphBuffer;
    private final ByteArrayBuffer modelBuffer;
    private final ByteArrayBuffer fieldBuffer;

    private final CompactWeightedPropertyBuilder compactWeightedPropertyBuilder;
    private final NFCompressedGraphLongPointers compressedGraphPointers;

    public NFWeightedCompressedGraphBuilder(NFGraphSpec graphSpec, NFWeightedBuildGraphNodeCache buildGraphNodeCache, NFGraphModelHolder modelHolder) {
        this.graphSpec = graphSpec;
        this.buildGraphNodeCache = buildGraphNodeCache;
        this.modelHolder = modelHolder;

        this.graphBuffer = new ByteArrayBuffer();
        this.modelBuffer = new ByteArrayBuffer();
        this.fieldBuffer = new ByteArrayBuffer();

        this.compactWeightedPropertyBuilder = new CompactWeightedPropertyBuilder(fieldBuffer);
        this.compressedGraphPointers = new NFCompressedGraphLongPointers();
    }

    public NFWeightedCompressedGraph buildGraph() {
        for (String nodeType : graphSpec.getNodeTypes()) {
            NFWeightedBuildGraphNodeList nodeOrdinals = buildGraphNodeCache.getNodes(nodeType);
            addNodeType(nodeType, nodeOrdinals);
        }
        return new NFWeightedCompressedGraph(graphSpec, modelHolder, graphBuffer.getData(), graphBuffer.length(), compressedGraphPointers);
    }

    private void addNodeType(String nodeType, NFWeightedBuildGraphNodeList nodes) {
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);
        long[] ordinalPointers = new long[nodes.size()];

        for (int i = 0; i < nodes.size(); i++) {
            NFWeightedBuildGraphNode node = nodes.get(i);
            if (node != null) {
                ordinalPointers[i] = graphBuffer.length();
                serializeNode(node, nodeSpec);
            } else {
                ordinalPointers[i] = -1;
            }
        }
        compressedGraphPointers.addPointers(nodeType, ordinalPointers);
    }

    private void serializeNode(NFWeightedBuildGraphNode node, NFNodeSpec nodeSpec) {
        for (NFPropertySpec propertySpec : nodeSpec.getPropertySpecs()) {
            serializeProperty(node, propertySpec);
        }
    }

    private void serializeProperty(NFWeightedBuildGraphNode node, NFPropertySpec propertySpec) {
        if (propertySpec.isConnectionModelSpecific()) {
            for (int i = 0; i < modelHolder.size(); i++) {
                serializeProperty(node, propertySpec, i, modelBuffer);
            }
            copyBuffer(modelBuffer, graphBuffer);
        } else {
            serializeProperty(node, propertySpec, 0, graphBuffer);
        }
    }

    private void serializeProperty(NFWeightedBuildGraphNode node, NFPropertySpec propertySpec, int connectionModelIndex, ByteArrayBuffer toBuffer) {
        if (propertySpec.isMultiple()) {
            serializeMultipleProperty(node, propertySpec, connectionModelIndex, toBuffer);
        } else {
            serializeSingleProperty(node, propertySpec, connectionModelIndex, toBuffer);
        }
    }

    private void serializeMultipleProperty(NFWeightedBuildGraphNode node, NFPropertySpec propertySpec, int connectionModelIndex, ByteArrayBuffer toBuffer) {
        WeightedOrdinalSet connections = node.getConnectionSet(connectionModelIndex, propertySpec);
        compactWeightedPropertyBuilder.buildProperty(connections);
        toBuffer.writeVInt((int) fieldBuffer.length());
        toBuffer.write(fieldBuffer);
        fieldBuffer.reset();
    }

    private void serializeSingleProperty(NFWeightedBuildGraphNode node, NFPropertySpec propertySpec, int connectionModelIndex, ByteArrayBuffer toBuffer) {
        WeightedOrdinalSet connection = node.getConnectionSet(connectionModelIndex, propertySpec);
        if (connection == null) {
            toBuffer.writeByte((byte) 0x80);
        } else {
            int[][] ordinalWithWeightAndLabel = connection.asArrayWithWeightAndLabel();
            // write ordinal
            toBuffer.writeVInt(ordinalWithWeightAndLabel[0][0]);
            // write weight
            toBuffer.writeVInt(ordinalWithWeightAndLabel[0][1]);
            // write label
            toBuffer.writeVInt(ordinalWithWeightAndLabel[0][2]);
        }
    }

    private void copyBuffer(ByteArrayBuffer from, ByteArrayBuffer to) {
        to.writeVInt((int) from.length());
        to.write(from);
        from.reset();
    }

}
