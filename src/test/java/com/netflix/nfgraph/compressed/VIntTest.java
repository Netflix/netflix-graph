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

import static org.junit.Assert.assertEquals;

import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.util.ByteArrayBuffer;
import com.netflix.nfgraph.util.ByteArrayReader;
import com.netflix.nfgraph.util.ByteData;

public class VIntTest {

    private int randomValues[];
    private ByteData data;
    private long seed;

    @Before
    public void setUp() {
        seed = System.currentTimeMillis();
        Random rand = new Random(seed);

        ByteArrayBuffer buf = new ByteArrayBuffer();
        randomValues = new int[rand.nextInt(10000)];

        for(int i=0;i<randomValues.length;i++) {
            randomValues[i] = rand.nextInt(Integer.MAX_VALUE);
            buf.writeVInt(randomValues[i]);
        }

        data = buf.getData();
    }

    @Test
    public void decodedValuesAreSameAsEncodedValues() {
        ByteArrayReader reader = new ByteArrayReader(data, 0);

        for(int i=0;i<randomValues.length;i++) {
            assertEquals("seed: " + seed, randomValues[i], reader.readVInt());
        }
    }

}
