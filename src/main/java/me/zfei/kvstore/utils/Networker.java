package me.zfei.kvstore.utils;

import me.zfei.kvstore.Server;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by zfei on 5/3/14.
 */
public class Networker {
    private static Logger logger = LoggerFactory.getLogger(Networker.class);
    Thread listener = null;
    private Server callbackServer = null;

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
                InputStream ins = socket.getInputStream();
                DataInputStream dins = new DataInputStream(ins);
                String msg = dins.readUTF();
                logger.info(String.format("Received: %s", msg));

                DataOutputStream outs = new DataOutputStream(socket.getOutputStream());
                callbackServer.onReceiveCommand(msg, outs);

                socket.close();
            } catch (IOException e) {
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

    public void startServerListener(final int port, Server callbackServer) {
        // will never start more than one listener
        if (listener != null) {
            logger.warn("Server already started");
            return;
        }

        this.callbackServer = callbackServer;

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
                        logger.debug(String.format("Started listening on port %d, waiting for connections...", port));
                        new ThreadedReceiver(serverSocket.accept()).start();
                    } catch (IOException e) {
                        logger.warn("Failed accepting client.");
                    }
                }
            }

        };

        listener.start();
    }

    public String unicastSend(ServerConfig serverConfig, String message) {
        return unicastSend(serverConfig.getHost(), serverConfig.getPort(), message);
    }

    public String unicastSend(String server, int port, String message) {
        logger.debug(String.format("Sending %s to %s:%d", message, server, port));

        try {
            Socket client = new Socket(server, port);
            OutputStream outToServer = client.getOutputStream();
            DataOutputStream out =
                    new DataOutputStream(outToServer);

            out.writeUTF(message);

            InputStream ins = client.getInputStream();
            DataInputStream dins = new DataInputStream(ins);
            String msg = dins.readUTF();

            logger.info(String.format("%s:%d says: %s", server, port, msg));

            client.close();
            return msg;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return "";
    }
}
