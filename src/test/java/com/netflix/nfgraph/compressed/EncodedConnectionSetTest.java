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

import static com.netflix.nfgraph.OrdinalIterator.NO_MORE_ORDINALS;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;

import com.netflix.nfgraph.OrdinalIterator;
import com.netflix.nfgraph.OrdinalSet;
import com.netflix.nfgraph.build.NFBuildGraphOrdinalSet;
import com.netflix.nfgraph.util.ByteData;

public abstract class EncodedConnectionSetTest {

    protected int totalOrdinals;
    protected Set<Integer> expectedOrdinals;
    protected ByteData data;
    protected long dataLength;

    protected Random rand;
    protected long seed;

    @Before
    public void setUp() {
        createRandom();

        totalOrdinals = rand.nextInt(maximumTotalOrdinals());

        OrdinalSet ordinals = generateExpectedOrdinals(totalOrdinals);

        data = generateCompressedData(ordinals);
    }

    protected abstract ByteData generateCompressedData(OrdinalSet ordinals);
    protected abstract OrdinalSet createOrdinalSet();
    protected abstract int maximumTotalOrdinals();

    private void createRandom() {
        seed = System.currentTimeMillis();
        rand = new Random(seed);
    }

    private OrdinalSet generateExpectedOrdinals(int totalOrdinals) {
        expectedOrdinals = new HashSet<Integer>();

        int numOrdinalsInSet = rand.nextInt(totalOrdinals);

        int ordinals[] = new int[numOrdinalsInSet];

        for(int i=0; i<ordinals.length; i++) {
            int ordinal = rand.nextInt(totalOrdinals);
            while(expectedOrdinals.contains(ordinal))
                ordinal = rand.nextInt(totalOrdinals);
            ordinals[i] = ordinal;
            expectedOrdinals.add(ordinal);
        }

        return new NFBuildGraphOrdinalSet(ordinals, ordinals.length);
    }


    @Test
    public void ordinalSetSizeIsCorrect() {
        OrdinalSet ordinalSet = createOrdinalSet();

        assertEquals("seed: " + seed, expectedOrdinals.size(), ordinalSet.size());
    }

    @Test
    public void ordinalSetContainsExpectedOrdinals() {
        OrdinalSet ordinalSet = createOrdinalSet();

        for(Integer expected : expectedOrdinals) {
            assertTrue("expected: " + expected.intValue() + " seed: " + seed, ordinalSet.contains(expected.intValue()));
        }
    }

    @Test
    public void ordinalSetContainsAll() {
        OrdinalSet ordinalSet = createOrdinalSet();

        int expected[] = new int[expectedOrdinals.size()];
        int expectedIdx = 0;

        for(Integer expect : expectedOrdinals) {
            if(rand.nextBoolean()) {
                expected[expectedIdx++] = expect.intValue();
            }
        }

        assertTrue("seed: " + seed, ordinalSet.containsAll(Arrays.copyOf(expected, expectedIdx)));
    }

    @Test
    public void ordinalSetContainsMostButNotAll() {
        OrdinalSet ordinalSet = createOrdinalSet();

        int unexpected[] = new int[expectedOrdinals.size() + 1];
        int unexpectedIdx = 0;

        boolean addedUnexpected = false;

        for(Integer expect : expectedOrdinals) {
            if(rand.nextBoolean()) {
                unexpected[unexpectedIdx++] = expect.intValue();
            }

            if(rand.nextInt(5) == 0) {
                unexpected[unexpectedIdx++] = generateUnexpectedOrdinal();
                addedUnexpected = true;
            }
        }

        if(!addedUnexpected) {
            unexpected[unexpectedIdx++] = generateUnexpectedOrdinal();
        }

        assertFalse("seed: " + seed, ordinalSet.containsAll(Arrays.copyOf(unexpected, unexpectedIdx)));
    }

    @Test
    public void ordinalSetDoesNotContainUnexpectedOrdinals() {
        OrdinalSet ordinalSet = createOrdinalSet();

        for(int i=0;i<totalOrdinals;i++) {
            if(!expectedOrdinals.contains(i)) {
                assertFalse("seed: " + seed, ordinalSet.contains(i));
            }
        }
    }

    @Test
    public void iteratorReturnsArray() {
        OrdinalSet ordinalSet = createOrdinalSet();

        int arr[] = ordinalSet.asArray();

        for(int ordinal : arr) {
            assertTrue("seed: " + seed, expectedOrdinals.contains(Integer.valueOf(ordinal)));
        }

        assertEquals(expectedOrdinals.size(), arr.length);
    }

    @Test
    public void iteratorReturnsAllExpectedOrdinalsOnce() {
        OrdinalIterator iter = createOrdinalSet().iterator();

        Set<Integer> returnedOrdinals = new HashSet<Integer>();
        int counter = 0;

        try {
            int ordinal = iter.nextOrdinal();

            while(ordinal != NO_MORE_ORDINALS) {
                counter++;
                assertTrue("seed: " + seed, expectedOrdinals.contains(ordinal));
                returnedOrdinals.add(ordinal);
                ordinal = iter.nextOrdinal();
            }
        } catch(Throwable t) {
            t.printStackTrace();
            fail("seed: " + seed);
        }

        assertEquals("seed: " + seed, expectedOrdinals.size(), returnedOrdinals.size());
        assertEquals("seed: " + seed, expectedOrdinals.size(), counter);
    }

    private int generateUnexpectedOrdinal() {
        int unexpectedOrdinal = rand.nextInt(totalOrdinals);
        while(expectedOrdinals.contains(unexpectedOrdinal))
            unexpectedOrdinal = rand.nextInt(totalOrdinals);
        return unexpectedOrdinal;
    }

}
