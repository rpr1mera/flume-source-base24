package com.woombatcg.hadoop.util.base24;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

class Base24ConfigReaderTest {
    @Test
    void getConfigurationFields() {

        byte[] bytes = new byte[0];

        try {
            bytes = Files.readAllBytes(
                    Paths.get("src/base24_cfg.example")
            );
        } catch (Exception e) {
            e.printStackTrace();
        }

        String s = new String(bytes);
//        System.out.println(s);
        Base24ConfigReader bcr = new Base24ConfigReader();
        List<String> matches = bcr.getConfigurationFields(s);

        System.out.println(matches);
    }

    @Test
    void buildConfig() {
        Base24ConfigReader bcr = new Base24ConfigReader();
        List<String> fields = Arrays.asList("P-1 M", "P-17 M", "S-102 C");
        List<Base24EMFField> config = bcr.buildConfig(fields);

        for (Base24EMFField field: config) {
            System.out.println(field);
        }
    }
}