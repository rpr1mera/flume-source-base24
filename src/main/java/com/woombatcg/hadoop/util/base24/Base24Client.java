package com.woombatcg.hadoop.util.base24;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import com.woombatcg.hadoop.util.base24.Functions;
import org.apache.commons.codec.binary.Hex;


public class Base24Client {
    public static void main(String[] args) {
        System.out.println("Hello, I'm a base24 client");

        try {
            byte[] fileBytes = readFile();
            String fileString = new String(fileBytes, StandardCharsets.UTF_8);
            System.out.println("hex string: " + fileString);
            byte[] msgBytes = Functions.hexStringToByteArray(fileString);

            int bytesCount = msgBytes.length;

            String s1 = new String(msgBytes, StandardCharsets.UTF_8);

            System.out.println("bytes: " + s1);

            // Create socket
            Socket socket = new Socket("127.0.0.1", 9000);
            OutputStream output = socket.getOutputStream();
            InputStream input = socket.getInputStream();

//            PrintWriter writer = new PrintWriter(output, true);
//            writer.println(fileString);
//
//            writer.close();

            int sleepTime = 500;
            int limit = 10;

            int i = 0;
            while (i < limit) {

                // Preparing the message to be sent
                int outputLength = 2 + msgBytes.length;
                ByteBuffer outputBuffer = ByteBuffer.allocate(outputLength);
                byte[] prefixSizeBytes = new byte[2];
                if (outputLength < 256) {
                    prefixSizeBytes[1] = BigInteger.valueOf(outputLength).toByteArray()[0];
                } else {
                    prefixSizeBytes = BigInteger.valueOf(outputLength).toByteArray();
                }
                outputBuffer.put(prefixSizeBytes);
                System.out.println("Sending msg: " + new String(msgBytes));
                outputBuffer.put(msgBytes);
                output.write(outputBuffer.array());

                // Reading the response
                byte[] inBytes = new byte[input.available()];
                int bytesRead = input.read(inBytes);
                System.out.println("Bytes read from socket: " + bytesRead);
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
                Paths.get("resources/input.0800")
        );
    }

}