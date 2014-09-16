package com.netflix.nfgraph.serializer;

import com.netflix.nfgraph.compressed.NFCompressedGraphIntPointers;
import com.netflix.nfgraph.compressed.NFCompressedGraphLongPointers;
import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;
import com.netflix.nfgraph.util.ByteArrayReader;
import com.netflix.nfgraph.util.SimpleByteArray;

import java.io.DataInputStream;
import java.io.IOException;

public class NFCompressedGraphPointersDeserializer {

    NFCompressedGraphPointers deserializePointers(DataInputStream dis) throws IOException {
        int numTypes = dis.readInt();

        /// Backwards compatibility:  The representation of the pointers is encoded as
        /// In order to maintain backwards compatibility of produced artifacts,
        /// if more than 32 bits is required to represent the pointers, then flag
        /// the sign bit in the serialized number of node types.
        if((numTypes & Integer.MIN_VALUE) != 0) {
            numTypes &= Integer.MAX_VALUE;
            return deserializeLongPointers(dis, numTypes & Integer.MAX_VALUE);
        }

        return deserializeIntPointers(dis, numTypes);
    }

    private NFCompressedGraphLongPointers deserializeLongPointers(DataInputStream dis, int numTypes) throws IOException {
        NFCompressedGraphLongPointers pointers = new NFCompressedGraphLongPointers();

        for(int i=0;i<numTypes;i++) {
            String nodeType = dis.readUTF();
            pointers.addPointers(nodeType, deserializeLongPointerArray(dis));
        }

        return pointers;
    }

    private long[] deserializeLongPointerArray(DataInputStream dis) throws IOException {
        int numNodes = dis.readInt();
        int numBytes = dis.readInt();

        byte data[] = new byte[numBytes];
        long pointers[] = new long[numNodes];

        dis.readFully(data);

        ByteArrayReader reader = new ByteArrayReader(new SimpleByteArray(data), 0);

        long currentPointer = 0;

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

    private NFCompressedGraphIntPointers deserializeIntPointers(DataInputStream dis, int numTypes) throws IOException {
        NFCompressedGraphIntPointers pointers = new NFCompressedGraphIntPointers();

        for(int i=0;i<numTypes;i++) {
            String nodeType = dis.readUTF();
            pointers.addPointers(nodeType, deserializeIntPointerArray(dis));
        }

        return pointers;
    }

    private int[] deserializeIntPointerArray(DataInputStream dis) throws IOException {
        int numNodes = dis.readInt();
        int numBytes = dis.readInt();

        byte data[] = new byte[numBytes];
        int pointers[] = new int[numNodes];

        dis.readFully(data);

        ByteArrayReader reader = new ByteArrayReader(new SimpleByteArray(data), 0);

        long currentPointer = 0;

        for(int i=0;i<numNodes;i++) {
            int vInt = reader.readVInt();
            if(vInt == -1) {
                pointers[i] = -1;
            } else {
                currentPointer += vInt;
                pointers[i] = (int)currentPointer;
            }
        }

        return pointers;
    }



}
