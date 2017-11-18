/*
 *  Copyright 2013-2017 Netflix, Inc.
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

package com.netflix.nfgraph.compressed;

import static com.netflix.nfgraph.OrdinalIterator.EMPTY_ITERATOR;
import static com.netflix.nfgraph.OrdinalSet.EMPTY_SET;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.netflix.nfgraph.NFGraph;
import com.netflix.nfgraph.NFGraphModelHolder;
import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
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

/**
 * A read-only, memory-efficient implementation of an {@link NFGraph}.  The connections for all nodes in the graph
 * are encoded into a single byte array.  The encoding for each set will be specified as either a {@link CompactOrdinalSet} or
 * {@link HashSetOrdinalSet}.  If it is more efficient, the actual encoding will be a {@link BitSetOrdinalSet}.<p>
 *
 * The offsets into the byte array where connections for each node are encoded are held in the {@link NFCompressedGraphPointers}.
 */
public class NFCompressedGraph extends NFGraph {

    private final NFCompressedGraphPointers pointers;
    private final ByteData data;
    private final long dataLength;

    public NFCompressedGraph(NFGraphSpec spec, NFGraphModelHolder modelHolder, ByteData data, long dataLength, NFCompressedGraphPointers pointers) {
        super(spec, modelHolder);
        this.data = data;
        this.dataLength = dataLength;
        this.pointers = pointers;
    }

    @Override
    protected int getConnection(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        ByteArrayReader reader = reader(nodeType, ordinal);

        if(reader != null) {
            NFPropertySpec propertySpec = pointReaderAtProperty(reader, nodeType, propertyName, connectionModelIndex);

            if(propertySpec != null) {
                if(propertySpec.isSingle())
                    return reader.readVInt();

                int firstOrdinal = iterator(nodeType, reader, propertySpec).nextOrdinal();
                if(firstOrdinal != OrdinalIterator.NO_MORE_ORDINALS)
                    return firstOrdinal;
            }
        }

        return -1;
    }

    @Override
    protected OrdinalSet getConnectionSet(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        ByteArrayReader reader = reader(nodeType, ordinal);

        if(reader != null) {
            NFPropertySpec propertySpec = pointReaderAtProperty(reader, nodeType, propertyName, connectionModelIndex);

            if (propertySpec != null) {
                return set(nodeType, reader, propertySpec);
            }
        }

        return EMPTY_SET;
    }

    @Override
    protected OrdinalIterator getConnectionIterator(int connectionModelIndex, String nodeType, int ordinal, String propertyName) {
        ByteArrayReader reader = reader(nodeType, ordinal);

        if(reader != null) {
            NFPropertySpec propertySpec = pointReaderAtProperty(reader, nodeType, propertyName, connectionModelIndex);

            if (propertySpec != null) {
                return iterator(nodeType, reader, propertySpec);
            }
        }

        return EMPTY_ITERATOR;
    }

    NFCompressedGraphPointers getPointers() {
        return pointers;
    }

    private OrdinalSet set(String nodeType, ByteArrayReader reader, NFPropertySpec propertySpec) {
        if(propertySpec.isSingle())
            return new SingleOrdinalSet(reader.readVInt());

        int size = reader.readVInt();

        if(size == -1) {
            int numBits = pointers.numPointers(propertySpec.getToNodeType());
            int numBytes = ((numBits - 1) / 8) + 1;
            reader.setRemainingBytes(numBytes);
            return new BitSetOrdinalSet(reader);
        }

        if(size == 0)
            return EMPTY_SET;

        if(propertySpec.isHashed()) {
            reader.setRemainingBytes(1 << (size - 1));
            return new HashSetOrdinalSet(reader);
        }

        reader.setRemainingBytes(size);
        return new CompactOrdinalSet(reader);
    }

    private OrdinalIterator iterator(String nodeType, ByteArrayReader reader, NFPropertySpec propertySpec) {
        if(propertySpec.isSingle())
            return new SingleOrdinalIterator(reader.readVInt());

        int size = reader.readVInt();

        if(size == -1) {
            int numBits = pointers.numPointers(propertySpec.getToNodeType());
            int numBytes = ((numBits - 1) / 8) + 1;
            reader.setRemainingBytes(numBytes);
            return new BitSetOrdinalIterator(reader);
        }

        if(size == 0)
            return EMPTY_ITERATOR;

        if(propertySpec.isHashed()) {
            reader.setRemainingBytes(1 << size);
            return new HashSetOrdinalIterator(reader);
        }

        reader.setRemainingBytes(size);
        return new CompactOrdinalIterator(reader);
    }

    private ByteArrayReader reader(String nodeType, int ordinal) {
        long pointer = pointers.getPointer(nodeType, ordinal);

        if(pointer == -1)
            return null;

        return new ByteArrayReader(data, pointer);
    }


    private NFPropertySpec pointReaderAtProperty(ByteArrayReader reader, String nodeType, String propertyName, int connectionModelIndex) {
        NFNodeSpec nodeSpec = graphSpec.getNodeSpec(nodeType);

        for (NFPropertySpec propertySpec : nodeSpec.getPropertySpecs()) {
            if (propertySpec.getName().equals(propertyName)) {
                if(propertySpec.isConnectionModelSpecific())
                    positionForModel(reader, connectionModelIndex, propertySpec);
                return propertySpec;
            } else {
                skipProperty(reader, propertySpec);
            }
        }

        throw new NFGraphException("Property " + propertyName + " is undefined for node type " + nodeType);
    }

    private void positionForModel(ByteArrayReader reader, int connectionModelIndex, NFPropertySpec propertySpec) {
        reader.setRemainingBytes(reader.readVInt());

        for(int i=0;i<connectionModelIndex;i++) {
            skipSingleProperty(reader, propertySpec);
        }
    }

    private void skipProperty(ByteArrayReader reader, NFPropertySpec propertySpec) {
        if(propertySpec.isConnectionModelSpecific()) {
            int size = reader.readVInt();
            reader.skip(size);
        } else {
            skipSingleProperty(reader, propertySpec);
        }
    }

    private void skipSingleProperty(ByteArrayReader reader, NFPropertySpec propertySpec) {
        if(propertySpec.isSingle()) {
            reader.readVInt();
            return;
        }

        int size = reader.readVInt();

        if(size == 0)
            return;

        if(size == -1) {
            int numBits = pointers.numPointers(propertySpec.getToNodeType());
            int numBytes = ((numBits - 1) / 8) + 1;
            reader.skip(numBytes);
            return;
        }

        if(propertySpec.isHashed()) {
            reader.skip(1 << (size - 1));
            return;
        }

        reader.skip(size);
    }

    public void writeTo(OutputStream os) throws IOException {
        NFCompressedGraphSerializer serializer = new NFCompressedGraphSerializer(graphSpec, modelHolder, pointers, data, dataLength);
        serializer.serializeTo(os);
    }

    public static NFCompressedGraph readFrom(InputStream is) throws IOException {
        return readFrom(is, null);
    }
    
    /**
     * When using a {@link ByteSegmentPool}, this method will borrow arrays used to construct the NFCompressedGraph from that pool.
     * <p>
     * Note that because the {@link ByteSegmentPool} is NOT thread-safe, this this call is also NOT thread-safe.
     * It is up to implementations to ensure that only a single update thread
     * is accessing this memory pool at any given time.
     */
    public static NFCompressedGraph readFrom(InputStream is, ByteSegmentPool memoryPool) throws IOException {
        NFCompressedGraphDeserializer deserializer = new NFCompressedGraphDeserializer();
        return deserializer.deserialize(is, memoryPool);
    }
    
    /**
     * When using a {@link ByteSegmentPool}, this method will return all borrowed arrays back to that pool.
     * <p>
     * Note that because the {@link ByteSegmentPool} is NOT thread-safe, this this call is also NOT thread-safe.
     * It is up to implementations to ensure that only a single update thread
     * is accessing this memory pool at any given time.
     */
    public void destroy() {
        if(data instanceof SegmentedByteArray)
            ((SegmentedByteArray) data).destroy();
    }

}
