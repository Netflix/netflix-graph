/*
 *  Copyright 2014 Netflix, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class NFCompressedGraphIntPointersTest {

    @Test
    public void test() {
        NFCompressedGraphIntPointers pointers = new NFCompressedGraphIntPointers();

        long ptr0 = ((long)Integer.MAX_VALUE + 1000);
        long ptr1 = ((long)Integer.MAX_VALUE * 2);

        pointers.addPointers("test", new int[] { (int)ptr0, (int)ptr1 });

        Assert.assertEquals(ptr0, pointers.getPointer("test", 0));
        Assert.assertEquals(ptr1, pointers.getPointer("test", 1));
    }

}
