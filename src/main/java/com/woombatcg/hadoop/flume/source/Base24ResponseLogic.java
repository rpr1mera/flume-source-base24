package com.woombatcg.hadoop.flume.source;

import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;

public class Base24ResponseLogic {
    public static byte[] handle(ISOMsg msg) {

        ISOMsg msgResponse = (ISOMsg) msg.clone();
        byte[] msgResponseBytes = new byte[]{};

        try {
            String mti = msg.getMTI();
            switch (mti) {
                case "0220": {
                    msgResponse.setMTI("0230");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                case "0800": {
                    msgResponse.setMTI("0810");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                default: {
                    msgResponseBytes = msgResponse.pack();
                }
            }
        } catch (ISOException e) {
            e.printStackTrace();
        }

        return  msgResponseBytes;
    }
}
