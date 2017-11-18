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
import com.netflix.nfgraph.compressed.HashSetOrdinalSet;
import com.netflix.nfgraph.util.ByteArrayBuffer;
import com.netflix.nfgraph.util.Mixer;

/**
 * This class is used by {@link NFCompressedGraphBuilder} to write sets of ordinals represented as as variable-byte hashed integer arrays.<p>
 * 
 * It is unlikely that this class will need to be used externally.
 * 
 * @see HashSetOrdinalSet
 */
public class HashedPropertyBuilder {

	private ByteArrayBuffer buf;
	
	public HashedPropertyBuilder(ByteArrayBuffer buf) {
		this.buf = buf;
	}
	
	public void buildProperty(OrdinalSet ordinals) {
	    if(ordinals.size() == 0)
	        return;
	    
		byte data[] = buildHashedPropertyData(ordinals);
		buf.write(data);
	}
	
	private byte[] buildHashedPropertyData(OrdinalSet ordinals) {
		byte data[] = new byte[calculateByteArraySize(ordinals)];
		
		OrdinalIterator iter = ordinals.iterator();
		
		int ordinal = iter.nextOrdinal();
		
		while(ordinal != NO_MORE_ORDINALS) {
			put(ordinal, data);
			ordinal = iter.nextOrdinal();
		}
		
		return data;
	}

	private void put(int value, byte data[]) {
	    value += 1;
	    
		int bucket = Mixer.hashInt(value) & (data.length - 1);

		if (data[bucket] != 0) {
			bucket = nextEmptyByte(data, bucket);
		}

		writeKey(value, bucket, data);
	}

	private void writeKey(int value, int offset, byte data[]) {
		int numBytes = calculateVIntSize(value);

		ensureSpaceIsAvailable(numBytes, offset, data);

		writeVInt(value, offset, data, numBytes);
	}

	private void writeVInt(int value, int offset, byte data[], int numBytes) {
		int b = (value >>> (7 * (numBytes - 1))) & 0x7F;
		data[offset] = (byte)b;
		offset = nextOffset(data.length, offset);

		for (int i = numBytes - 2; i >= 0; i--) {
			b = (value >>> (7 * i)) & 0x7F;
			data[offset] = (byte)(b | 0x80);
			offset = nextOffset(data.length, offset);
		}
	}
	
	private int nextOffset(int length, int offset) {
		offset++;
		if (offset == length)
			offset = 0;
		return offset;
	}

	private int previousOffset(int length, int offset) {
		offset--;
		if (offset == -1)
			offset = length - 1;
		return offset;
	}
	
	private void ensureSpaceIsAvailable(int requiredSpace, int offset, byte data[]) {
		int copySpaces = 0;
		int foundSpace = 1;
		int currentOffset = offset;

		while (foundSpace < requiredSpace) {
			currentOffset = nextOffset(data.length, currentOffset);
			if (data[currentOffset] == 0) {
				foundSpace++;
			} else {
				copySpaces++;
			}
		}

		int moveToOffset = currentOffset;
		currentOffset = previousOffset(data.length, currentOffset);

		while (copySpaces > 0) {
			if (data[currentOffset] != 0) {
				data[moveToOffset] = data[currentOffset];
				copySpaces--;
				moveToOffset = previousOffset(data.length, moveToOffset);
			}
			currentOffset = previousOffset(data.length, currentOffset);
		}
	}
	
	private int nextEmptyByte(byte data[], int offset) {
		while (data[offset] != 0) {
			offset = nextOffset(data.length, offset);
		}
		return offset;
	}

	private int calculateByteArraySize(OrdinalSet ordinals) {
		int numPopulatedBytes = calculateNumPopulatedBytes(ordinals.iterator());

		return calculateByteArraySizeAfterLoadFactor(numPopulatedBytes);
	}

	private int calculateNumPopulatedBytes(OrdinalIterator ordinalIterator) {
		int totalSize = 0;
		int ordinal = ordinalIterator.nextOrdinal();

		while(ordinal != NO_MORE_ORDINALS) {
			totalSize += calculateVIntSize(ordinal + 1);
			ordinal = ordinalIterator.nextOrdinal();
		}

		return totalSize;
	}
	
	private int calculateVIntSize(int value) {
		int numBitsSet = numBitsUsed(value);
		return ((numBitsSet - 1) / 7) + 1;
	}

	private int calculateByteArraySizeAfterLoadFactor(int numPopulatedBytes) {
		int desiredSizeAfterLoadFactor = (numPopulatedBytes * 4) / 3;

		int nextPowerOfTwo = 1 << numBitsUsed(desiredSizeAfterLoadFactor);
		return nextPowerOfTwo;
	}

	private int numBitsUsed(int value) {
		return 32 - Integer.numberOfLeadingZeros(value);
	}

}
