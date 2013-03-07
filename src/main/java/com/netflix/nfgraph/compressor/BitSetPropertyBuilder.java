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

package com.netflix.nfgraph.compressor;

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressed.BitSetOrdinalSet;
import com.netflix.nfgraph.util.ByteArrayBuffer;

/**
 * This class is used by {@link NFCompressedGraphBuilder} to write sets of ordinals represented with bit sets.<p/>
 * 
 * It is unlikely that this class will need to be used externally.
 * 
 * @see BitSetOrdinalSet
 */
public class BitSetPropertyBuilder {

	private final ByteArrayBuffer buf;
	
	public BitSetPropertyBuilder(ByteArrayBuffer buf) {
		this.buf = buf;
	}
	
	public void buildProperty(OrdinalSet ordinals, int numBits) {
		byte[] data = buildBitSetData(numBits, ordinals.iterator());
		buf.write(data);
	}

	private byte[] buildBitSetData(int numBits, OrdinalIterator iter) {
		int numBytes = ((numBits - 1) / 8) + 1;
        byte data[] = new byte[numBytes];
		
		int ordinal = iter.nextOrdinal();
		
		while(ordinal != NO_MORE_ORDINALS) {
			data[ordinal >> 3] |= (byte)(1 << (ordinal & 0x07));
			ordinal = iter.nextOrdinal();
		}
		
		return data;
	}
	
}
