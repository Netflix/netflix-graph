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

import java.util.Arrays;

import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressed.CompactOrdinalSet;
import com.netflix.nfgraph.util.ByteArrayBuffer;

/**
 * This class is used by {@link NFCompressedGraphBuilder} to write sets of ordinals represented as variable-byte deltas.<p/>
 * 
 * It is unlikely that this class will need to be used externally.
 * 
 * @see CompactOrdinalSet
 */
public class CompactPropertyBuilder {

	private final ByteArrayBuffer buf;
	
	public CompactPropertyBuilder(ByteArrayBuffer buf) {
		this.buf = buf;
	}
	
	public void buildProperty(OrdinalSet ordinalSet) {
		int connectedOrdinals[] = ordinalSet.asArray();
		Arrays.sort(connectedOrdinals);
		
		int previousOrdinal = 0;
		
		for(int i=0;i<connectedOrdinals.length;i++) {
			buf.writeVInt(connectedOrdinals[i] - previousOrdinal);
			previousOrdinal = connectedOrdinals[i];
		}
	}
	
}
