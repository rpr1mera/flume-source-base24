package com.woombatcg.hadoop.util.base24;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import static com.woombatcg.hadoop.util.base24.Functions.encodeHexString;


public class Base24Client2 {
    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Usage: java B24client.jar host port number_of_trx wait_time_x_trxs(ms)");
        }

        try {

            String hostname = args[0];
            int port = Integer.parseInt(args[1]);
            int limit = Integer.parseInt(args[2]);
            int sleepTime = Integer.parseInt(args[3]);
            String inputFilePath = args[4];

            // Create client connection socket
            System.out.printf("Connecting to %s using port %d\n", hostname, port);
            Socket socket = new Socket(args[0], Integer.parseInt(args[1]));
            OutputStream connectionOutputStream = socket.getOutputStream();
            InputStream connectionInputStream = socket.getInputStream();

            List<String> textLines = readFile(inputFilePath);

            List<String> textLinesDecoded = new ArrayList<>();

            // Generate each message
            for (String textLine : textLines) {
                byte[] lineBytes = textLine.getBytes();
                String lineString = new String(lineBytes, StandardCharsets.UTF_8);
                System.out.println("hex string: " + lineString);

                byte[] msgBytes = Functions.hexStringToByteArray(lineString);
                String decodedString = new String(msgBytes, StandardCharsets.UTF_8);
                System.out.println("decoded string bytes: " + decodedString);

                textLinesDecoded.add(decodedString);
            }

            // Message iteration logic
            int i = 0;
            while (i < limit) {
                for (String textLineDecoded: textLinesDecoded) {
                    byte[] msgBytes = textLineDecoded.getBytes(StandardCharsets.UTF_8);
                    int msgBytesCount = msgBytes.length;

                    // Main logic
                    // Preparing the message to be sent
                    int outputLength = 2 + msgBytesCount;
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
                    byte[] outputBufferArray = outputBuffer.array();
                    System.out.println("Sending bytes: ");
                    String hexString = encodeHexString(outputBufferArray);
                    System.out.println(hexString);
                    connectionOutputStream.write(outputBufferArray);

                    // Reading the response
                    byte[] inBytes = new byte[connectionInputStream.available()];
                    int bytesRead = connectionInputStream.read(inBytes);
                    System.out.println("Bytes read from socket: " + bytesRead);
//                String s = new String(inBytes);
                    System.out.println(new String(inBytes));
                    Thread.sleep(sleepTime);

                    i++;
                }
            }

        }catch (Exception e)  {
            e.printStackTrace();
        }
    }

    public static List<String> readFile(String inputFile) throws IOException {
         byte[] fileBytes = Files.readAllBytes(
                Paths.get(inputFile)
        );

        String fileText = new String(fileBytes, StandardCharsets.UTF_8);
        String[] str_array = fileText.split("\n");

        List<String> list = new ArrayList<String>(Arrays.asList(str_array));
        list.remove("");
        return list;
    }

}