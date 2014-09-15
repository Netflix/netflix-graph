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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressor.HashedPropertyBuilder;
import com.netflix.nfgraph.util.ByteArrayBuffer;
import com.netflix.nfgraph.util.ByteArrayReader;
import com.netflix.nfgraph.util.ByteData;

import org.junit.Test;

public class HashSetTest extends EncodedConnectionSetTest {

    @Override
    protected ByteData generateCompressedData(OrdinalSet ordinals) {
        ByteArrayBuffer buf = new ByteArrayBuffer();
        HashedPropertyBuilder builder = new HashedPropertyBuilder(buf);

        builder.buildProperty(ordinals);

        dataLength = buf.length();

        return buf.getData();
    }

    @Override
    protected OrdinalSet createOrdinalSet() {
        ByteArrayReader reader = new ByteArrayReader(data, 0, dataLength);
        return new HashSetOrdinalSet(reader);
    }

    @Override
    protected int maximumTotalOrdinals() {
        return 100000;
    }

    @Test
    public void singleOrdinal127IsSizedAppropriately() {
    	ByteArrayBuffer buf = new ByteArrayBuffer();

    	HashedPropertyBuilder builder = new HashedPropertyBuilder(buf);

    	builder.buildProperty(new SingleOrdinalSet(127));

    	ByteArrayReader reader = new ByteArrayReader(buf.getData(), 0, buf.length());

    	OrdinalSet set = new HashSetOrdinalSet(reader);

    	assertTrue(set.contains(127));
    	assertFalse(set.contains(128));
    }


}
