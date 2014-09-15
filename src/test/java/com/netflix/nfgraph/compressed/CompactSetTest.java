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

import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.compressor.CompactPropertyBuilder;
import com.netflix.nfgraph.util.ByteArrayBuffer;
import com.netflix.nfgraph.util.ByteArrayReader;
import com.netflix.nfgraph.util.ByteData;

public class CompactSetTest extends EncodedConnectionSetTest {

    @Override
    protected ByteData generateCompressedData(OrdinalSet ordinals) {
        ByteArrayBuffer buf = new ByteArrayBuffer();
        CompactPropertyBuilder builder = new CompactPropertyBuilder(buf);

        builder.buildProperty(ordinals);

        dataLength = buf.length();
        return buf.getData();
    }

    @Override
    protected OrdinalSet createOrdinalSet() {
        ByteArrayReader reader = new ByteArrayReader(data, 0, dataLength);
        return new CompactOrdinalSet(reader);
    }

    @Override
    protected int maximumTotalOrdinals() {
        return 1000;
    }

}
