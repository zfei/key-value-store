package me.zfei.kvstore;

import me.zfei.kvstore.utils.Networker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zfei on 5/3/14.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private Networker networker;
    private int port;

    private Map<String, String> dataStore = new HashMap<String, String>();

    public Server(int port) {
        networker = new Networker();
        this.port = port;
    }

    public void start() {
        networker.startListener(port);
    }

    public void shutdown() {
        System.exit(0);
    }


}
