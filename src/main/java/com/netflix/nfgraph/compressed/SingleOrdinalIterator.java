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

/**
 * An implementation of {@link OrdinalIterator} which "iterates" over a single ordinal.
 */
public class SingleOrdinalIterator implements OrdinalIterator {

    private final int ordinal;
    private boolean returned;
    
    public SingleOrdinalIterator(int ordinal) {
        this.ordinal = ordinal;
    }
    
    @Override
    public int nextOrdinal() {
        if(returned)
            return NO_MORE_ORDINALS;
        
        returned = true;
        return ordinal;
    }

    @Override
    public void reset() {
        returned = false;
    }

    @Override
    public OrdinalIterator copy() {
        return new SingleOrdinalIterator(ordinal);
    }

    @Override
    public boolean isOrdered() {
        return true;
    }

}
