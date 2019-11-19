package com.woombatcg.flume.source;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Base24Client {
    public static void main(String[] args) {
        System.out.println("Hello, I'm a base24 client");

//        try {
//            for (byte item: readFile()) {
//                System.out.print(item);
//            }
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

        try {
            byte[] fileBytes = readFile();
            String fileString = new String(fileBytes, "utf-8");
            System.out.print(fileString);
            Socket socket = new Socket("127.0.0.1", 8000);
            OutputStream output = socket.getOutputStream();

            PrintWriter writer = new PrintWriter(output, true);
            writer.println(fileString);

            writer.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static byte[] readFile() throws IOException {
        return Files.readAllBytes(
                Paths.get("/home/rprimera/woombat/davivienda/base24/dev/flume-source-base24/src/com/woombatcg/flume/source/0230_respuesta_notificacion_avance.txt")
        );
    }
}
