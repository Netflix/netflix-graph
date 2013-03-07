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

/**
 * An implementation of {@link OrdinalSet} containing a single ordinal.
 */
public class SingleOrdinalSet extends OrdinalSet {

    private final int ordinal;
    
    public SingleOrdinalSet(int ordinal) {
        this.ordinal = ordinal;
    }

    @Override
    public boolean contains(int value) {
        return ordinal == value;
    }

    @Override
    public int[] asArray() {
        return new int[] { ordinal };
    }

    @Override
    public OrdinalIterator iterator() {
        return new SingleOrdinalIterator(ordinal);
    }

    @Override
    public int size() {
        return 1;
    }
    
}
