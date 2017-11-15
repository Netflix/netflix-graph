/*
 *  Copyright 2014-2017 Netflix, Inc.
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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class SegmentedByteArray implements ByteData {
    private byte[][] segments;
    private final ByteSegmentPool memoryPool;
    private final int log2OfSegmentSize;
    private final int bitmask;
    private long length;

    public SegmentedByteArray(int log2OfSegmentSize) {
        this(new ByteSegmentPool(log2OfSegmentSize));
    }
    
    public SegmentedByteArray(ByteSegmentPool memoryPool) {
        this.segments = new byte[2][];
        this.memoryPool = memoryPool;
        this.log2OfSegmentSize = memoryPool.getLog2OfSegmentSize();
        this.bitmask = (1 << log2OfSegmentSize) - 1;
        this.length = 0;
    }

    /**
     * Set the byte at the given index to the specified value
     */
    public void set(long index, byte value) {
        int segmentIndex = (int)(index >> log2OfSegmentSize);
        ensureCapacity(segmentIndex);
        segments[segmentIndex][(int)(index & bitmask)] = value;
    }

    /**
     * Get the value of the byte at the specified index.
     */
    public byte get(long index) {
        return segments[(int)(index >>> log2OfSegmentSize)][(int)(index & bitmask)];
    }

    /**
     * For a SegmentedByteArray, this is a faster copy implementation.
     *
     * @param src
     * @param srcPos
     * @param destPos
     * @param length
     */
    public void copy(SegmentedByteArray src, long srcPos, long destPos, long length) {
        int segmentLength = 1 << log2OfSegmentSize;
        int currentSegment = (int)(destPos >>> log2OfSegmentSize);
        int segmentStartPos = (int)(destPos & bitmask);
        int remainingBytesInSegment = segmentLength - segmentStartPos;

        while(length > 0) {
            int bytesToCopyFromSegment = (int)Math.min(remainingBytesInSegment, length);
            ensureCapacity(currentSegment);
            int copiedBytes = src.copy(srcPos, segments[currentSegment], segmentStartPos, bytesToCopyFromSegment);

            srcPos += copiedBytes;
            length -= copiedBytes;
            segmentStartPos = 0;
            remainingBytesInSegment = segmentLength;
            currentSegment++;
        }

    }

    /**
     * copies exactly data.length bytes from this SegmentedByteArray into the provided byte array
     *
     * @return the number of bytes copied
     */
    public int copy(long srcPos, byte[] data, int destPos, int length) {
        int segmentSize = 1 << log2OfSegmentSize;
        int remainingBytesInSegment = (int)(segmentSize - (srcPos & bitmask));
        int dataPosition = destPos;

        while(length > 0) {
            byte[] segment = segments[(int)(srcPos >>> log2OfSegmentSize)];

            int bytesToCopyFromSegment = Math.min(remainingBytesInSegment, length);

            System.arraycopy(segment, (int)(srcPos & bitmask), data, dataPosition, bytesToCopyFromSegment);

            dataPosition += bytesToCopyFromSegment;
            srcPos += bytesToCopyFromSegment;
            remainingBytesInSegment = segmentSize - (int)(srcPos & bitmask);
            length -= bytesToCopyFromSegment;
        }

        return dataPosition - destPos;
    }


    public void readFrom(InputStream is, long length) throws IOException {
        int segmentSize = 1 << log2OfSegmentSize;
        int segment = 0;
        while(length > 0) {
            ensureCapacity(segment);
            long bytesToCopy = Math.min(segmentSize, length);
            long bytesCopied = 0;
            while(bytesCopied < bytesToCopy) {
                bytesCopied += is.read(segments[segment], (int)bytesCopied, (int)(bytesToCopy - bytesCopied));
            }
            segment++;
            length -= bytesCopied;
        }
    }

    @Override
    public void writeTo(OutputStream os, long length) throws IOException {
        writeTo(os, 0, length);
    }

    /**
     * Write a portion of this data to an OutputStream.
     */
    public void writeTo(OutputStream os, long startPosition, long len) throws IOException {
        int segmentSize = 1 << log2OfSegmentSize;
        int remainingBytesInSegment = segmentSize - (int)(startPosition & bitmask);
        long remainingBytesInCopy = len;

        while(remainingBytesInCopy > 0) {
            long bytesToCopyFromSegment = Math.min(remainingBytesInSegment, remainingBytesInCopy);

            os.write(segments[(int)(startPosition >>> log2OfSegmentSize)], (int)(startPosition & bitmask), (int)bytesToCopyFromSegment);

            startPosition += bytesToCopyFromSegment;
            remainingBytesInSegment = segmentSize - (int)(startPosition & bitmask);
            remainingBytesInCopy -= bytesToCopyFromSegment;
        }
    }

    /**
     * Ensures that the segment at segmentIndex exists
     *
     * @param segmentIndex
     */
    private void ensureCapacity(int segmentIndex) {
        while(segmentIndex >= segments.length) {
            segments = Arrays.copyOf(segments, segments.length * 3 / 2);
        }

        long numSegmentsPopulated = length >> log2OfSegmentSize;

        for(long i=numSegmentsPopulated; i <= segmentIndex; i++) {
            segments[(int)i] = memoryPool.getSegment();
            length += 1 << log2OfSegmentSize;
        }
    }

    @Override
    public long length() {
        return length;
    }
    
    /**
     * Note that this is NOT thread safe.
     */
    public void destroy() {
        for(byte[] segment : segments) {
            memoryPool.returnSegment(segment);
        }
    }

}
