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
