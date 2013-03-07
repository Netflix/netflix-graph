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

package com.netflix.nfgraph.compressed;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.util.ByteArrayReader;

/**
 * An implementation of {@link OrdinalIterator} returned for ordinals represented as variable-byte hashed integer arrays in an {@link NFCompressedGraph}.
 * 
 * @see HashSetOrdinalSet
 */
public class HashSetOrdinalIterator implements OrdinalIterator {

    private final ByteArrayReader reader;
    private final int beginOffset;
    private int offset = 0;
    private boolean firstValue;
    
    public HashSetOrdinalIterator(ByteArrayReader reader) {
        this.reader = reader;
        seekBeginByte();
        this.beginOffset = offset;
        firstValue = true;
    }

    @Override
    public int nextOrdinal() {
        seekBeginByte();
        
        if(offset == beginOffset) {
            if(!firstValue)
                return NO_MORE_ORDINALS;
            firstValue = false;
        }
        
        int value = reader.getByte(offset);
        nextOffset();
        
        while((reader.getByte(offset) & 0x80) != 0) {
            value <<= 7;
            value |= reader.getByte(offset) & 0x7F;
            nextOffset();
        }
        
        return value - 1;
    }

    @Override
    public void reset() { 
        offset = beginOffset;
        firstValue = true;
    }

    @Override
    public OrdinalIterator copy() {
        return new HashSetOrdinalIterator(reader);
    }

    @Override
    public boolean isOrdered() {
        return false;
    }
    
    private void nextOffset() {
        offset++;
        if(offset >= reader.length()) {
            offset = 0;
        }
    }
    
    private void seekBeginByte() {
        while((reader.getByte(offset) & 0x80) != 0 || reader.getByte(offset) == 0)
            nextOffset();
    }
}
