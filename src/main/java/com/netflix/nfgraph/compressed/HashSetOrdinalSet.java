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
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayReader;
import com.netflix.nfgraph.util.Mixer;

/**
 * An implementation of {@link OrdinalSet}, returned for connections represented as variable-byte hashed integer arrays in an {@link NFCompressedGraph}.<p>
 *
 * A variable-byte hashed integer array representation contains between one and five bytes per connection.  The ordinal for each
 * connection is hashed into a byte array, then represented as a variant on the variable-byte integers used in the {@link CompactOrdinalSet}.<p>
 *
 * The byte array can be thought of as a open-addressed hash table, with each byte representing a single bucket.  Because
 * values may be represented in more than one byte, single values may spill over into multiple buckets.  The beginning of the
 * value is indicated by an unset sign bit, and will be located at or after the bucket to which it is hashed.  If the value's
 * first bit is not located at the hashed position, it will be located in a position after the bucket with no empty buckets in between.<p>
 *
 * This implementation provides <code>O(1)</code> time for <code>contains()</code>, but is not as memory-efficient as a {@link CompactOrdinalSet}.<p>
 *
 * This representation for a connection set can be configured for an {@link NFPropertySpec} using {@link NFPropertySpec#HASH}.
 *
 * @see <a href="https://github.com/Netflix/netflix-graph/wiki/Compact-representations">Compact Representations</a>
 *
 */
public class HashSetOrdinalSet extends OrdinalSet {

    private final ByteArrayReader reader;
    private int size = Integer.MIN_VALUE;

    public HashSetOrdinalSet(ByteArrayReader reader) {
        this.reader = reader;
    }

    @Override
    public OrdinalIterator iterator() {
        return new HashSetOrdinalIterator(reader.copy());
    }

    @Override
    public boolean contains(int value) {
        value += 1;

        int offset = (Mixer.hashInt(value) & ((int)reader.length() - 1));

        offset = seekBeginByte(offset);

        while(reader.getByte(offset) != 0) {
            int readValue = reader.getByte(offset);
            offset = nextOffset(offset);

            while((reader.getByte(offset) & 0x80) != 0) {
                readValue <<= 7;
                readValue |= reader.getByte(offset) & 0x7F;
                offset = nextOffset(offset);
            }

            if(readValue == value)
                return true;
        }

        return false;
    }

    @Override
    public int size() {
        if(size == Integer.MIN_VALUE)
            size = countHashEntries();
        return size;
    }

    private int seekBeginByte(int offset) {
        while((reader.getByte(offset) & 0x80) != 0)
            offset = nextOffset(offset);
        return offset;
    }

    private int nextOffset(int offset) {
        offset++;
        if(offset >= reader.length()) {
            offset = 0;
        }
        return offset;
    }

    private int countHashEntries() {
        int counter = 0;
        for(int i=0;i<reader.length();i++) {
            byte b = reader.getByte(i);
            if(b != 0 && (b & 0x80) == 0)
                counter++;
        }
        return counter;
    }

}
