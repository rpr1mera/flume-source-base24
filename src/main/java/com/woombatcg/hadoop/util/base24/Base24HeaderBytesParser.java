package com.woombatcg.hadoop.util.base24;

import com.google.common.base.Charsets;
import org.jpos.iso.packager.BASE24Packager;

import java.util.Arrays;
import java.util.HashMap;

public class Base24HeaderBytesParser {

    public static HashMap<String, String> parse(byte[] b) {
        HashMap<String, String> map = new HashMap<String, String>();

        String productIndicator = new String(
                Arrays.copyOfRange(b, 0, 2),
                Charsets.US_ASCII
        );

        String releaseNumber = new String(
                Arrays.copyOfRange(b, 2, 4),
                Charsets.US_ASCII
        );

        String status = new String(
                Arrays.copyOfRange(b, 4, 7),
                Charsets.US_ASCII
        );

        String originatorCode = new String(
                new byte[]{b[7]},
                Charsets.US_ASCII
        );

        String responderCode = new String(
                new byte[]{b[8]},
                Charsets.US_ASCII
        );

        map.put("product_indicator", productIndicator);
        map.put("release_number", releaseNumber);
        map.put("status", status);
        map.put("originator_code", originatorCode);
        map.put("responder_code", responderCode);

        return map;
    }

}
