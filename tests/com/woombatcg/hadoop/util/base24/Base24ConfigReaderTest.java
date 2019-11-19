package com.woombatcg.hadoop.util;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

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
}