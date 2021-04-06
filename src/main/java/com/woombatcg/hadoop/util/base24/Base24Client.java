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
import java.util.Scanner;

import com.woombatcg.hadoop.util.base24.Functions;
import org.apache.commons.codec.binary.Hex;


public class Base24Client {
    public static void main(String[] args) {
        if (args.length!=2) {
            System.out.println("Usage: java B24client.jar host port number_of_trx wait_time_x_trxs(ms)");
        }
        Scanner sc = new Scanner(System.in);
        try {

            String hostname = args[0];
            int port = Integer.parseInt(args[1]);
            System.out.printf("Connecting to %s using port %d\n", hostname, port);

            byte[] fileBytes = readFile();
            String fileString = new String(fileBytes, StandardCharsets.UTF_8);
            System.out.println("hex string: " + fileString);

            byte[] msgBytes = Functions.hexStringToByteArray(fileString);
            String s1 = new String(msgBytes, StandardCharsets.UTF_8);
            System.out.println("bytes: " + s1);

            int bytesCount = msgBytes.length;

            // Create socket
            Socket socket = new Socket(args[0], Integer.parseInt(args[1]));
            OutputStream output = socket.getOutputStream();
            InputStream input = socket.getInputStream();

//            PrintWriter writer = new PrintWriter(output, true);
//            writer.println(fileString);
//
//            writer.close();

            do {
                try {
                    int i = 0;
//                    System.out.println("How many trx?:");
//                    int limit = sc.nextInt();
                    int limit = Integer.parseInt(args[2]);
//                    System.out.println("Wait time:");
//                    int sleepTime = sc.nextInt();
                    int sleepTime = Integer.parseInt(args[3]);
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
                    break;
                } catch (InterruptedException ie) {
                    output.close();
                    System.out.println("Ended by user");
                    System.exit(0);
                }
            }while(true);
        }catch (Exception e)  {
            e.printStackTrace();
        }
    }

    public static byte[] readFile() throws IOException {
        return Files.readAllBytes(
                Paths.get("resources/input")
        );
    }

}