package com.netflix.nfgraph.serializer;

import com.netflix.nfgraph.compressed.NFCompressedGraphIntPointers;
import com.netflix.nfgraph.compressed.NFCompressedGraphLongPointers;
import com.netflix.nfgraph.compressed.NFCompressedGraphPointers;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

public class NFCompressedGraphPointersSerializerTest {

    @Test
    public void dataLengthLessthan4GBUsesIntegerPointers() throws IOException {
        NFCompressedGraphLongPointers pointers = new NFCompressedGraphLongPointers();
        pointers.addPointers("test", new long[] { 1, 2, 3 });

        NFCompressedGraphPointersSerializer serializer = new NFCompressedGraphPointersSerializer(pointers, (long)Integer.MAX_VALUE * 2);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        serializer.serializePointers(dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        NFCompressedGraphPointers deserialized = new NFCompressedGraphPointersDeserializer().deserializePointers(dis);

        Assert.assertTrue(deserialized instanceof NFCompressedGraphIntPointers);
    }

    @Test
    public void dataLengthGreaterThan4GBUsesLongPointers() throws IOException {
        NFCompressedGraphLongPointers pointers = new NFCompressedGraphLongPointers();
        pointers.addPointers("test", new long[] { 1, 2, 3 });

        NFCompressedGraphPointersSerializer serializer = new NFCompressedGraphPointersSerializer(pointers, (long)Integer.MAX_VALUE * 3);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        serializer.serializePointers(dos);

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()));

        NFCompressedGraphPointers deserialized = new NFCompressedGraphPointersDeserializer().deserializePointers(dis);

        Assert.assertTrue(deserialized instanceof NFCompressedGraphLongPointers);
    }

}
