package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.*;
import com.netflix.nfgraph.exception.NFGraphException;
import com.netflix.nfgraph.serializer.NFCompressedGraphDeserializer;
import com.netflix.nfgraph.serializer.NFCompressedGraphSerializer;
import com.netflix.nfgraph.spec.NFGraphSpec;
import com.netflix.nfgraph.spec.NFNodeSpec;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayReader;
import com.netflix.nfgraph.util.ByteData;
import com.netflix.nfgraph.util.ByteSegmentPool;
import com.netflix.nfgraph.util.SegmentedByteArray;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class NFWeightedCompressedGraph extends NFGraph {

    private final NFCompressedGraphPointers pointers;
    private final ByteData data;
    private final long dataLength;

    public NFWeightedCompressedGraph(NFGraphSpec spec, NFGraphModelHolder modelHolder, ByteData data, long dataLength, NFCompressedGraphPointers pointers) {
        super(spec, modelHolder);
        this.data = data;
        this.dataLength = dataLength;
        this.pointers = pointers;
    }

    public static NFWeightedCompressedGraph readFrom(InputStream is) throws IOException {
        return readFrom(is, null);
    }

    /**
     * When using a {@link ByteSegmentPool}, this method will borrow arrays used to construct the NFWeightedCompressedGraph from that pool.
     * <p>
     * Note that because the {@link ByteSegmentPool} is NOT thread-safe, this call is also NOT thread-safe.
     * It is up to implementations to ensure that only a single update thread
     * is accessing this memory pool at any given time.
     */
    public static NFWeightedCompressedGraph readFrom(InputStream is, ByteSegmentPool memoryPool) throws IOException {
        NFCompressedGraphDeserializer deserializer = new NFCompressedGraphDeserializer();
        return deserializer.deserializeWeightedGraph(is, memoryPool);
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

    public WeightedOrdinalIterator getWeightedConnectionIterator(String nodeType, int ordinal, String propertyName) {
        return getConnectionIterator(0, nodeType, ordinal, propertyName);
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
        ByteArrayReader reader = reader(nodeType, ordinal);
        if (reader != null) {
            NFPropertySpec propertySpec = pointReaderAtProperty(reader, nodeType, propertyName, connectionModelIndex);

            if (propertySpec != null) {
                if (propertySpec.isSingle()) {
                    // read ordinal
                    int o = reader.readVInt();
                    // read weight
                    reader.readVInt();
                    // read property
                    reader.readVInt();
                    return o;
                }

                int firstOrdinal = iterator(nodeType, reader, propertySpec).nextOrdinal();
                if (firstOrdinal != OrdinalIterator.NO_MORE_ORDINALS) {
                    return firstOrdinal;
                }
            }
        }
        return -1;
    }

    @Override
    protected WeightedOrdinalSet getConnectionSet(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        ByteArrayReader reader = reader(nodeType, ordinal);
        if (reader != null) {
            NFPropertySpec propertySpec = pointReaderAtProperty(reader, nodeType, propertyName, connectionModelIndex);
            if (propertySpec != null) {
                return set(nodeType, reader, propertySpec);
            }
        }
        return WeightedOrdinalSet.EMPTY_SET;
    }

    @Override
    protected WeightedOrdinalIterator getConnectionIterator(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        ByteArrayReader reader = reader(nodeType, ordinal);
        if (reader != null) {
            NFPropertySpec propertySpec = pointReaderAtProperty(reader, nodeType, propertyName, connectionModelIndex);
            if (propertySpec != null) {
                return iterator(nodeType, reader, propertySpec);
            }
        }
        return WeightedOrdinalIterator.EMPTY_WEIGHTED_ITERATOR;
    }

    NFCompressedGraphPointers getPointers() {
        return pointers;
    }

    private WeightedOrdinalSet set(String nodeType, ByteArrayReader reader, NFPropertySpec propertySpec) {
        if (propertySpec.isSingle()) {
            return new SingleWeightedOrdinalSet(reader.readVInt(), reader.readVInt(), reader.readVInt());
        }
        int size = reader.readVInt();
        if (size == 0) {
            return WeightedOrdinalSet.EMPTY_SET;
        }
        reader.setRemainingBytes(size);
        return new CompactWeightedOrdinalSet(reader);
    }

    private WeightedOrdinalIterator iterator(String nodeType, ByteArrayReader reader, NFPropertySpec propertySpec) {
        if (propertySpec.isSingle()) {
            return new SingleWeightedOrdinalIterator(reader.readVInt(), reader.readVInt(), reader.readVInt());
        }
        int size = reader.readVInt();
        if (size == 0) {
            return WeightedOrdinalIterator.EMPTY_WEIGHTED_ITERATOR;
        }
        reader.setRemainingBytes(size);
        return new CompactWeightedOrdinalIterator(reader);
    }

    private ByteArrayReader reader(String nodeType, int ordinal) {
        long pointer = pointers.getPointer(nodeType, ordinal);
        if (pointer == -1) {
            return null;
        }
        return new ByteArrayReader(data, pointer);
    }

    private NFPropertySpec pointReaderAtProperty(ByteArrayReader reader, String nodeType, String propertyName, int connectionModelIndex) {
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);

        for (NFPropertySpec propertySpec : nodeSpec.getPropertySpecs()) {
            if (propertySpec.getName().equals(propertyName)) {
                if (propertySpec.isConnectionModelSpecific()) {
                    positionForModel(reader, connectionModelIndex, propertySpec);
                }
                return propertySpec;
            } else {
                skipProperty(reader, propertySpec);
            }
        }
        throw new NFGraphException("Property " + propertyName + " is undefined for node type " + nodeType);
    }

    private void positionForModel(ByteArrayReader reader, int connectionModelIndex, NFPropertySpec propertySpec) {
        reader.setRemainingBytes(reader.readVInt());
        for (int i = 0; i < connectionModelIndex; i++) {
            skipSingleProperty(reader, propertySpec);
        }
    }

    private void skipProperty(ByteArrayReader reader, NFPropertySpec propertySpec) {
        if (propertySpec.isConnectionModelSpecific()) {
            int size = reader.readVInt();
            reader.skip(size);
        } else {
            skipSingleProperty(reader, propertySpec);
        }
    }

    private void skipSingleProperty(ByteArrayReader reader, NFPropertySpec propertySpec) {
        if (propertySpec.isSingle()) {
            // read ordinal
            reader.readVInt();
            // read weight
            reader.readVInt();
            // read property
            reader.readVInt();
            return;
        }

        int size = reader.readVInt();

        if (size == 0) {
            return;
        }
        reader.skip(size);
    }

    public void writeTo(OutputStream os) throws IOException {
        NFCompressedGraphSerializer serializer = new NFCompressedGraphSerializer(graphSpec, modelHolder, pointers, data, dataLength);
        serializer.serializeTo(os);
    }

    /**
     * When using a {@link ByteSegmentPool}, this method will return all borrowed arrays back to that pool.
     * <p>
     * Note that because the {@link ByteSegmentPool} is NOT thread-safe, this this call is also NOT thread-safe.
     * It is up to implementations to ensure that only a single update thread
     * is accessing this memory pool at any given time.
     */
    public void destroy() {
        if (data instanceof SegmentedByteArray) ((SegmentedByteArray) data).destroy();
    }

}
