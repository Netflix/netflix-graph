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

package com.netflix.nfgraph.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

import com.netflix.nfgraph.compressor.NFCompressedGraphBuilder;

/**
 * A <code>ByteArrayBuffer</code> is used by the {@link NFCompressedGraphBuilder} to write data to a byte array.<p/>
 *
 * It is unlikely that this class will need to be used externally.
 */
public class ByteArrayBuffer {

    private byte data[];

    private int pointer;

    public ByteArrayBuffer() {
        this(1024);
    }

    public ByteArrayBuffer(int initialSize) {
        this.data = new byte[initialSize];
        this.pointer = 0;
    }

    /**
     * Copies the contents of the specified buffer into this buffer at the current position.
     */
    public void write(ByteArrayBuffer buf) {
        while(data.length < pointer + buf.length())
            grow();

        System.arraycopy(buf.backingArray(), 0, data, pointer, buf.length());
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
    public int length() {
        return pointer;
    }

    /**
     * Sets the length of the written data to 0.
     */
    public void reset() {
        pointer = 0;
    }

    /**
     * @return a byte array containing a copy of the written data.  The length of the byte array will be exactly equal to the written data.
     */
    public byte[] getData() {
        return Arrays.copyOf(data, length());
    }

    /**
     * Writes a byte of data.
     */
    public void writeByte(byte b) {
        if(pointer == data.length)
            grow();
        data[pointer++] = b;
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
        os.write(data, 0, pointer);
    }

    private void grow() {
        this.data = Arrays.copyOf(data, (int)(((long)data.length * 5) / 4));
    }

    private byte[] backingArray() {
        return data;
    }

}
