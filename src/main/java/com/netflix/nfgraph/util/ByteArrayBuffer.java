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

package com.netflix.nfgraph.util;

import com.netflix.nfgraph.compressor.NFCompressedGraphBuilder;

import java.io.IOException;
import java.io.OutputStream;

/**
 * A <code>ByteArrayBuffer</code> is used by the {@link NFCompressedGraphBuilder} to write data to a byte array.<p>
 *
 * It is unlikely that this class will need to be used externally.
 */
public class ByteArrayBuffer {

    private final SegmentedByteArray data;

    private long pointer;

    public ByteArrayBuffer() {
        this.data = new SegmentedByteArray(new ByteSegmentPool(14));
        this.pointer = 0;
    }

    /**
     * @deprecated Use zero-argument constructor instead.
     */
    @Deprecated
    public ByteArrayBuffer(int initialSize) {
        this();
    }

    /**
     * Copies the contents of the specified buffer into this buffer at the current position.
     */
    public void write(ByteArrayBuffer buf) {
        data.copy(buf.data, 0, pointer, buf.length());
        pointer += buf.length();
    }

    /**
     * Writes a variable-byte encoded integer to the byte array.
     */
    public void writeVInt(int value) {
        if(value < 0) {
            writeByte((byte)0x80);
            return;
        }

        if(value > 0x0FFFFFFF) writeByte((byte)(0x80 | ((value >>> 28))));
        if(value > 0x1FFFFF)   writeByte((byte)(0x80 | ((value >>> 21) & 0x7F)));
        if(value > 0x3FFF)     writeByte((byte)(0x80 | ((value >>> 14) & 0x7F)));
        if(value > 0x7F)       writeByte((byte)(0x80 | ((value >>>  7) & 0x7F)));

        writeByte((byte)(value & 0x7F));
    }

    /**
     * The current length of the written data, in bytes.
     */
    public long length() {
        return pointer;
    }

    /**
     * Sets the length of the written data to 0.
     */
    public void reset() {
        pointer = 0;
    }

    /**
     * @return The underlying SegmentedByteArray containing the written data.
     */
    public SegmentedByteArray getData() {
        return data;
    }

    /**
     * Writes a byte of data.
     */
    public void writeByte(byte b) {
        data.set(pointer++, b);
    }

    /**
     * Writes each byte of data, in order.
     */
    public void write(byte[] data) {
        for(int i=0;i<data.length;i++) {
            writeByte(data[i]);
        }
    }

    /**
     * Copies the written data to the given <code>OutputStream</code>
     */
    public void copyTo(OutputStream os) throws IOException {
        data.writeTo(os, 0, pointer);
    }

}
