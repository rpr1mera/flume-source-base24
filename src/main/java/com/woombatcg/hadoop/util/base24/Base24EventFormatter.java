package com.woombatcg.hadoop.util.base24;

import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.commons.codec.binary.Hex;
import org.jpos.iso.*;
import org.jpos.iso.packager.XMLPackager;

import java.util.HashMap;
import java.util.LinkedHashMap;

public class Base24EventFormatter {

    public static byte[] handler(ISOMsg msg, String format) throws Exception {

        byte[] response = new byte[]{};

        switch (format.toLowerCase()) {

            case "json": {
                try {
                    ISOMsg msgCopy = (ISOMsg) msg.clone();
                    LinkedHashMap<String, String> map = new LinkedHashMap<String, String>();

                    for (int i = 0; i < 129; i++) {

                        ISOComponent ic = msg.getComponent(i);
                        String key = (
                                i < 65 ? "p-" : "s-"
                        ) + i;


                        String value;

                        if (ic == null) {
//                            System.out.println("ic was null");
                            value = null;

                            map.put(
                                    key,
                                    value
                            );
                        }
                        else if (ic instanceof ISOField) {
                            value = new String(ic.getBytes());
                            map.put(key, value);
                        }
                        else if (ic instanceof ISOBinaryField) {
                            System.out.println(i + " is a binary field");
                            value = Hex.encodeHexString(ic.getBytes());
                            map.put(key, value);
                        }
                    }

                    GsonBuilder builder = new GsonBuilder();
                    builder.serializeNulls();
                    builder.disableHtmlEscaping();

                    Gson gson = builder.create();
                    response = gson.toJson(map).getBytes();
                    break;


                } catch (Exception e) {
                    e.printStackTrace();
                }


                break;
            }

            case "xml": {
                ISOMsg msgCopy = (ISOMsg) msg.clone();
                msgCopy.setPackager(new XMLPackager());
                response = msgCopy.pack();

                break;
            }

            case "base24": {
                response = msg.pack();
                break;
            }

            default: throw new Exception("Unsupported output format: " + format.toLowerCase());
        }

        return response;

    }

}
