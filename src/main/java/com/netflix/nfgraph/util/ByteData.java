package com.netflix.nfgraph.util;

import java.io.IOException;
import java.io.OutputStream;


public interface ByteData {

    public void set(long idx, byte b);

    public byte get(long idx);

    public long length();

    public void writeTo(OutputStream os, long length) throws IOException;

}
