package com.woombatcg.hadoop.flume.source;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Base24ResponseLogic {
    private static final Logger logger = LoggerFactory
            .getLogger(Base24Source.class);

    static ISOMsg handleSpecjsonArray(ISOMsg msgResponse, JSONArray jsonArray) throws ISOException {
        String mti = msgResponse.getMTI();
        try{
            logger.info("Matching configuration keys to MTI=" + mti);
            for (Object o : jsonArray) {
                JSONObject jsonObject = (JSONObject) o;
                String key = (String) jsonObject.get("key");
                logger.info("Key=" + key);

                if (key.equals(mti)) {
                    logger.info("Configuration found for MTI=" + mti);

                    String resp = (String) jsonObject.get("resp");
                    logger.info("Setting response code to " + resp);
                    msgResponse.setMTI(resp);

                    JSONArray fieldsList = (JSONArray) jsonObject.get("fields");
                    logger.info("Response fields configuration = " + fieldsList.toJSONString());


                    for (Object item : fieldsList) {
                        JSONObject fieldsJson = (JSONObject) item;
                        int field_id = ((Long) fieldsJson.get("field_id")).intValue();
                        String value = (String) fieldsJson.get("value");
                        logger.info("Setting field id p-" + field_id + " to value=" + value);
                        msgResponse.set(field_id, value);
                    }
                    break;
                }
            }
        } catch (ISOException e) {
            e.printStackTrace();
            throw e;
        }
        return msgResponse;
    }


    public static byte[] handle(ISOMsg msg, JSONArray jsonArray) throws ISOException {

        JSONParser jsonParser = new JSONParser();
        ISOMsg msgResponse = (ISOMsg) msg.clone();
        byte[] msgResponseBytes;

        try {
            msgResponse =handleSpecjsonArray(msgResponse,jsonArray);
            msgResponseBytes = msgResponse.pack();
        } catch (ISOException e) {
            e.printStackTrace();
            throw e;
        }

        return  msgResponseBytes;
    }
}
