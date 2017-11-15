/*
 *  Copyright 2017 Netflix, Inc.
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
package com.netflix.nfgraph.util;

import java.util.Arrays;
import java.util.LinkedList;

/**
 * This is a memory pool which can be used to allocate and reuse memory for deserialized NFCompressedGraph data.
 * 
 * Note that this is NOT thread safe, and it is up to implementations to ensure that only a single update thread
 * is accessing this memory pool at any given time.
 */
public class ByteSegmentPool {
    
    private final LinkedList<byte[]> pooledSegments;
    private final int log2OfSegmentSize;
    
    public ByteSegmentPool(int log2OfSegmentSize) {
        this.pooledSegments = new LinkedList<>();
        this.log2OfSegmentSize = log2OfSegmentSize;
    }
    
    public int getLog2OfSegmentSize() {
        return log2OfSegmentSize;
    }
    
    public byte[] getSegment() {
        if(pooledSegments.isEmpty())
            return new byte[1 << log2OfSegmentSize];
        try {
            byte[] segment = pooledSegments.removeFirst();
            Arrays.fill(segment, (byte)0);
            return segment;
        } catch(NullPointerException ex) {
            throw ex;
        }
    }
    
    public void returnSegment(byte[] segment) {
        if(segment != null)
            pooledSegments.addLast(segment);
    }
}
