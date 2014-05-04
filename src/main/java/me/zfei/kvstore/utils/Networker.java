package me.zfei.kvstore.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Scanner;

/**
 * Created by zfei on 5/3/14.
 */
public class Networker {
    private static Logger logger = LoggerFactory.getLogger(Networker.class);
    Thread listener = null;

    class ThreadedReceiver extends Thread {
        private Socket socket;

        ThreadedReceiver(Socket socket) {
            logger.debug("Connection established");
            this.socket = socket;
        }

        @Override
        public void run() {

            logger.debug("Threaded receiver is running");

            try {
                System.out.println(socket.getLocalPort());
                InputStream ins = socket.getInputStream();

                String msg = new DataInputStream(ins).readUTF();

                logger.info(String.format("Received: %s", msg));

                DataOutputStream outs = new DataOutputStream(socket.getOutputStream());
                outs.writeUTF("ACCEPTED");
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                try {
                    if (socket != null) socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void startListener(final int port) {
        // will never start more than one listener
        if (listener != null)
            return;

        listener = new Thread() {

            @Override
            public void run() {
                ServerSocket serverSocket = null;
                try {
                    serverSocket = new ServerSocket(port);
                } catch (IOException e) {
                    logger.warn(String.format("Failed binding port %d. Will exit.", port));
                    System.exit(1);
                }

                while (true) {
                    try {
                        logger.debug("Waiting for connections...");
                        new ThreadedReceiver(serverSocket.accept()).start();
                    } catch (IOException e) {
                        logger.warn("Failed accepting client.");
                    }
                }
            }

        };

        listener.start();
    }

    public void unicastSend(String server, int port, String message) {
        try {
            Socket client = new Socket(server, port);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out =
                    new DataOutputStream(outToServer);

            out.writeUTF(message);

            InputStream inFromServer = client.getInputStream();
            DataInputStream in =
                    new DataInputStream(inFromServer);

            logger.info("Server says " + in.readUTF());

            client.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Networker server = new Networker();
        server.startListener(5000);

        Networker client = new Networker();
        Scanner sc = new Scanner(System.in, "UTF-8");
        while (sc.hasNext()) {
            String next = sc.nextLine();
            logger.debug("sending " + next);
            client.unicastSend("localhost", 5000, next);
        }
    }
}
