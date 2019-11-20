package com.woombatcg.hadoop.util.base24;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class FunctionsTest {

    @Test
    void hexStringToByteArray() throws Exception {
//        byte[] bytes = Functions.hexStringToByteArray("04ba");
        byte[] bytes = Functions.hexStringToByteArray("04ba49534f30323630303030353030323230423233384334383132454531");
        String s = Arrays.toString(bytes);
        System.out.println(s);
        s = new String(bytes, "UTF-8");
        System.out.println(s);
        System.out.println("\u00ba");
    }
}