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
package com.netflix.nfgraph.util;

import java.io.IOException;
import java.io.OutputStream;

public class SimpleByteArray implements ByteData {

    private final byte data[];

    public SimpleByteArray(int length) {
        this.data = new byte[length];
    }

    public SimpleByteArray(byte[] data) {
        this.data = data;
    }

    @Override
    public void set(long idx, byte b) {
        data[(int)idx] = b;
    }

    @Override
    public byte get(long idx) {
        return data[(int)idx];
    }

    @Override
    public long length() {
        return data.length;
    }

    @Override
    public void writeTo(OutputStream os, long length) throws IOException {
        os.write(data, 0, (int)length);
    }

}
