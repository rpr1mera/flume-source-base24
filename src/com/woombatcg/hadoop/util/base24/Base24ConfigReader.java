package com.woombatcg.hadoop.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

public class Base24ConfigReader {
    public List<String> getConfigurationFields(String s) {
        Pattern p = Pattern.compile("[PS]-\\d+\\s+[MC]");
        Matcher m = p.matcher(s);
        List<String> matches = new ArrayList<String>();

        while (m.find()) {
            matches.add(m.group());
        }
        return matches;
    };

    public List<HashMap<String, String>> buildConfig() {

    }
};