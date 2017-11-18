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
import com.netflix.nfgraph.compressor.NFCompressedGraphBuilder;
import com.netflix.nfgraph.spec.NFPropertySpec;
import com.netflix.nfgraph.util.ByteArrayReader;

/**
 * An implementation of {@link OrdinalSet}, returned for connections represented as a bit set in an {@link NFCompressedGraph}.<p>
 *
 * A bit set representation contains a single bit per ordinal in the type to which the connections point.  If the bit at the
 * position for a given ordinal is set, then there is a connection to that ordinal in this set.<p>
 *
 * Because determining membership in a set requires only checking whether the bit at a given position is set, <code>contains()</code>
 * is an <code>O(1)</code> operation.<p>
 *
 * This representation will automatically be chosen for a set by the {@link NFCompressedGraphBuilder} when it requires fewer bytes than
 * the configured representation (either {@link NFPropertySpec#COMPACT} or {@link NFPropertySpec#HASH}).
 *
 * @see <a href="https://github.com/Netflix/netflix-graph/wiki/Compact-representations">Compact Representations</a>
 */
public class BitSetOrdinalSet extends OrdinalSet {

    private final ByteArrayReader reader;

    public BitSetOrdinalSet(ByteArrayReader reader) {
        this.reader = reader;
    }

    @Override
    public boolean contains(int value) {
        int offset = value >>> 3;
        int mask = 1 << (value & 0x07);

        if(offset >= reader.length())
            return false;

        return (reader.getByte(offset) & mask) != 0;
    }

    @Override
    public OrdinalIterator iterator() {
        return new BitSetOrdinalIterator(reader);
    }

    @Override
    public int size() {
        int cardinalitySum = 0;
        for(int i=0;i<(reader.length());i++) {
            cardinalitySum += BITS_SET_TABLE[reader.getByte(i) & 0xFF];
        }
        return cardinalitySum;
    }

    private static final int BITS_SET_TABLE[] = new int[256];
    static {
        for(int i=0;i<256;i++) {
            BITS_SET_TABLE[i] = (i & 1) + BITS_SET_TABLE[i / 2];
        }
    }

}
