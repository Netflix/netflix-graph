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

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressed.NFCompressedGraph;

/**
 * Used by the {@link NFCompressedGraph}, and various {@link OrdinalSet} and {@link OrdinalIterator} implementations to read the encoded graph data.<p/>
 * 
 * It is unlikely that this class will be required externally.
 */
public class ByteArrayReader {

    private final byte data[];
    
    private int pointer;
    private int startByte;
    private int endByte = Integer.MAX_VALUE;
    
    public ByteArrayReader(byte data[], int pointer) {
        this.data = data;
        this.pointer = pointer;
        this.startByte = pointer;
        this.endByte = data.length;
    }
    
    private ByteArrayReader(byte data[], int startByte, int endByte) {
        this.data = data;
        this.startByte = startByte;
        this.endByte = endByte;
        this.pointer = startByte;
    }
    
    /**
     * @return the byte value at the given offset.
     */
    public byte getByte(int offset) {
        return data[startByte + offset];
    }
    
    /**
     * Set the current offset of this reader.
     */
    public void setPointer(int pointer) {
        this.pointer = pointer;
    }
    
    /**
     * Increment the current offset of this reader by numBytes. 
     */
    public void skip(int numBytes) {
        pointer += numBytes;
    }
    
    /**
     * @return a variable-byte integer at the current offset.  The offset is incremented by the size of the returned integer. 
     */
    public int readVInt() {
        if(pointer >= endByte)
            return -1;
        
        byte b = readByte();
        
        if(b == (byte) 0x80)
            return -1;
        
        int value = b & 0x7F;
        while ((b & 0x80) != 0) {
          b = readByte();
          value <<= 7;
          value |= (b & 0x7F);
        }
        
        return value;
    }
    
    /**
     * @return the byte at the current offset.  The offset is incremented by one.
     */
    public byte readByte() {
        return data[pointer++];
    }

    /**
     * Sets the start byte of this reader to the current offset, then sets the end byte to the current offset + <code>remainingBytes</code> 
     */
    public void setRemainingBytes(int remainingBytes) {
        this.startByte = pointer;
        this.endByte = pointer + remainingBytes;
    }
    
    /**
     * Sets the current offset of this reader to the start byte.
     */
    public void reset() {
        this.pointer = startByte;
    }
    
    /**
     * @return the length of this reader. 
     */
    public int length() {
        return endByte - startByte;
    }
    
    /**
     * @return a copy of this reader.  The copy will have the same underlying byte array, start byte, and end byte, but the current offset will be equal to the start byte.
     */
    public ByteArrayReader copy() {
        return new ByteArrayReader(data, startByte, endByte);
    }
    
    
}
