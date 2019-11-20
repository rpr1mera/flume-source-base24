package com.woombatcg.hadoop.util.base24;

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

    public List<Base24EMFField> buildConfig(List<String> items) {
        List<Base24EMFField> config = new ArrayList<Base24EMFField>();

        for (String item: items) {
            String[] parts = item.split("\\s");
            String requirement = parts[1];
            String[] left = parts[0].split("-");
            String identifier = left[0];
            int index = Integer.parseInt(left[1]);

            config.add(new Base24EMFField(identifier, index, requirement));
        }

        return config;
    }
};