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
