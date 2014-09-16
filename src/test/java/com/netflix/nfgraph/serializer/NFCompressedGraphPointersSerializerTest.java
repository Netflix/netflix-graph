/*
 *  Copyright 2014 Netflix, Inc.
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
