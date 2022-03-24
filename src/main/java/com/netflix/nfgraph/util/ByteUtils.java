package com.netflix.nfgraph.util;

public class ByteUtils {

    private static final int CHAR_PACKING_TO_INT_MASK = 0x00000_FFFF;
    public static final long INT_PACKING_MASK = 0x0000_0000_FFFF_FFFFL;

    public static int packTwoChars(final char left, final char right) {
        return ((left & CHAR_PACKING_TO_INT_MASK) << 16) | (right & CHAR_PACKING_TO_INT_MASK);
    }

    public static long packTwoInts(int left, int right) {
        return ((left & INT_PACKING_MASK) << 32) | (right & INT_PACKING_MASK);
    }

    public static int getLeftInt(long packed) {
        return (int) (packed >>> 32);
    }

    public static int getRightInt(long packed) {
        return (int) (packed & INT_PACKING_MASK);
    }

    public static char getRightChar(final int packed) {
        return (char) packed;
    }

    public static char getLeftChar(final int packed) {
        return (char) (packed >>> 16);
    }
}
