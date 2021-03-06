package com.woombatcg.hadoop.util.base24;

import org.jpos.iso.ISOMsg;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

public class Functions {
    public static byte[] hexStringToByteArray(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    public List<byte[]> split(byte[] array, byte[] delimiter) {
        List<byte[]> byteArrays = new LinkedList<byte[]>();
        if (delimiter.length == 0) {
            return byteArrays;
        }
        int begin = 0;

        outer: for (int i = 0; i < array.length - delimiter.length + 1; i++) {
            for (int j = 0; j < delimiter.length; j++) {
                if (array[i + j] != delimiter[j]) {
                    continue outer;
                }
            }

            // If delimiter is at the beginning then there will not be any data.
            if (begin != i)
                byteArrays.add(Arrays.copyOfRange(array, begin, i));
            begin = i + delimiter.length;
        }

        // delimiter at the very end with no data following?
        if (begin != array.length)
            byteArrays.add(Arrays.copyOfRange(array, begin, array.length));

        return byteArrays;
    }

    public static String byteToHex(byte num) {
        char[] hexDigits = new char[2];
        hexDigits[0] = Character.forDigit((num >> 4) & 0xF, 16);
        hexDigits[1] = Character.forDigit((num & 0xF), 16);
        return new String(hexDigits);
    }

    public static String encodeHexString(byte[] byteArray) {
        StringBuffer hexStringBuffer = new StringBuffer();
        for (int i = 0; i < byteArray.length; i++) {
            hexStringBuffer.append(byteToHex(byteArray[i]));
        }
        return hexStringBuffer.toString();
    }
}
