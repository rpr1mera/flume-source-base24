package com.woombatcg.hadoop.util.base24;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.woombatcg.hadoop.util.base24.Functions;

public class Base24Client {
    public static void main(String[] args) {
        System.out.println("Hello, I'm a base24 client");

        try {
            byte[] fileBytes = readFile();
            String fileString = new String(fileBytes, StandardCharsets.UTF_8);
            System.out.println("hex string: " + fileString);
            byte[] bytes = Functions.hexStringToByteArray(fileString);

            String s1 = new String(bytes, StandardCharsets.UTF_8);

            System.out.println("bytes: " + s1);
            Socket socket = new Socket("127.0.0.1", 9000);
            OutputStream output = socket.getOutputStream();
            InputStream input = socket.getInputStream();

//            PrintWriter writer = new PrintWriter(output, true);
//            writer.println(fileString);
//
//            writer.close();

            int sleepTime = 500;
            int limit = 10;
            byte[] inBytes = new byte[1210];

            int i = 0;
            while (i < limit) {
                output.write(bytes);
                input.read(inBytes);
//                String s = new String(inBytes);
                System.out.println(new String(inBytes));
                Thread.sleep(sleepTime);
                i++;
            }

            output.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] readFile() throws IOException {
        return Files.readAllBytes(
                Paths.get("resources/input")
        );
    }

}