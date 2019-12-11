package com.woombatcg.hadoop.flume.source;

import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;

public class Base24ResponseLogic {
    public static byte[] handle(ISOMsg msg) throws ISOException {

        ISOMsg msgResponse = (ISOMsg) msg.clone();
        byte[] msgResponseBytes = new byte[]{};

        try {
            String mti = msg.getMTI();
            switch (mti) {
                // Message Class: Authorization
                case "0100" : {
                    msgResponse.setMTI("0110");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }
                case "0121":
                case "0120" : {
                    msgResponse.setMTI("0130");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                // Message Class: Financial Transaction
                case "0200" : {
                    msgResponse.setMTI("0210");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }
                case "0221":
                case "0220" : {
                    msgResponse.setMTI("0230");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                // Message Class: Reversal
                case "0402" : {
                    msgResponse.setMTI("0412");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }
                case "0420":
                case "0421" : {
                    msgResponse.setMTI("0430");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                // Reconciliation Control
                case "0500" : {
                    msgResponse.setMTI("0510");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }
                case "0521":
                case "0520" : {
                    msgResponse.setMTI("0530");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                // Network Management
                case "0800": {
                    msgResponse.setMTI("0810");
                    msgResponse.set(39, "00");
                    msgResponseBytes = msgResponse.pack();
                    break;
                }

                default: {
                    // Revise this
//                    msgResponseBytes = msgResponse.pack();
                    throw new ISOException(String.format("MTI '%s' is not supported by Base24", mti));
                }
            }
        } catch (ISOException e) {
            e.printStackTrace();
            throw e;
        }

        return  msgResponseBytes;
    }
}
