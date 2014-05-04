package me.zfei.kvstore;

import me.zfei.kvstore.utils.CommandAction;
import me.zfei.kvstore.utils.Networker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by zfei on 5/3/14.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private Networker networker;
    private int port;

    private Map<String, String> dataStore = new HashMap<String, String>();
    private Set<String> removedSet = new HashSet<String>();

    public Server(int port) {
        networker = new Networker();
        this.port = port;
    }

    public void start() {
        networker.startServerListener(port, this);
    }

    public void shutdown() {
        System.exit(0);
    }

    public void onReceiveCommand(String command, DataOutputStream outs) {
        String[] commandParts = command.split("\\s+");
        CommandAction actionType = Client.getActionType(commandParts);

        switch (actionType) {
            case SEARCH:
                break;
            case SEARCHALL:
                break;
            case DELETE:
                break;
            case GET:
                break;
            case INSERT:
                break;
            case UPDATE:
                break;
        }

        try {
            outs.writeUTF("ACCEPTED");
        } catch (IOException e) {
            logger.warn("Cannot respond to sender");
        }
    }
}
