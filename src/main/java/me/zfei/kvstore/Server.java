package me.zfei.kvstore;

import com.google.gson.Gson;
import me.zfei.kvstore.utils.CommandAction;
import me.zfei.kvstore.utils.Networker;
import me.zfei.kvstore.utils.QueryResult;
import me.zfei.kvstore.utils.ResultEntry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * Created by zfei on 5/3/14.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private Networker networker;
    private int port;

    private Map<String, String> datastore = new HashMap<String, String>();
    private Set<String> tombstones = new HashSet<String>();
    private Map<String, Long> timestamps = new HashMap<String, Long>();

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

    public boolean updateTimestamp(String key, long timestamp) {
        if (!timestamps.containsKey(key) || timestamps.get(key) < timestamp) {
            logger.debug(String.format("Original timestamp: %d, received: %d",
                    timestamps.get(key) != null ? timestamps.get(key) : -1, timestamp));
            timestamps.put(key, timestamp);
            return true;
        }

        return false;
    }

    public void onReceiveCommand(String message, DataOutputStream outs) throws IOException {
        String command = message.substring(0, message.lastIndexOf('@'));
        long timestamp = Long.parseLong(message.substring(message.lastIndexOf('@') + 1));
        String[] commandParts = command.trim().split("\\s+");
        CommandAction actionType = Client.getActionType(commandParts);

        switch (actionType) {
            case SEARCH:
                searchExistence(commandParts, outs);
                break;
            case SHOWALL:
                showAll(outs);
                break;
            case DELETE:
                if (updateTimestamp(commandParts[1], timestamp))
                    createTombstone(commandParts, outs);
                else
                    respondFailure(outs, "Received stale data");
                break;
            case GET:
                getFromDatastore(commandParts, outs);
                break;
            case INSERT:
                if (updateTimestamp(commandParts[1], timestamp))
                    updateOrInsert(commandParts, outs);
                else
                    respondFailure(outs, "Received stale data");
                break;
            case UPDATE:
                if (updateTimestamp(commandParts[1], timestamp))
                    updateOrInsert(commandParts, outs);
                else
                    respondFailure(outs, "Received stale data");
                break;
            default:
                respondFailure(outs, "Command not understood");
                break;
        }
    }

    private void searchExistence(String[] commandParts, DataOutputStream outs) throws IOException {
        getFromDatastore(commandParts, outs);
    }

    private void showAll(DataOutputStream outs) throws IOException {
        List<ResultEntry> entries = new ArrayList<ResultEntry>();

        for (Map.Entry<String, String> entry : datastore.entrySet())
            if (!tombstones.contains(entry.getKey()))
                entries.add(new ResultEntry(entry.getKey(), entry.getValue()));

        QueryResult result = new QueryResult(true, entries);
        outs.writeUTF(new Gson().toJson(result));
    }

    private void createTombstone(String[] commandParts, DataOutputStream outs) throws IOException {
        tombstones.add(commandParts[1]);
        respondSuccess(outs);
    }

    private void respondSuccess(DataOutputStream outs) throws IOException {
        QueryResult result = new QueryResult(true);
        outs.writeUTF(new Gson().toJson(result));
    }

    private void respondFailure(DataOutputStream outs, String reason) throws IOException {
        QueryResult result = new QueryResult(false, reason);
        outs.writeUTF(new Gson().toJson(result));
    }

    private void getFromDatastore(String[] commandParts, DataOutputStream outs) throws IOException {
        String key = commandParts[1];

        if (!tombstones.contains(key) && datastore.containsKey(key)) {
            List<ResultEntry> entries = new ArrayList<ResultEntry>();
            entries.add(new ResultEntry(key, datastore.get(key)));
            QueryResult result = new QueryResult(true, entries);
            outs.writeUTF(new Gson().toJson(result));
        } else
            respondFailure(outs, "Key not found");
    }

    private void updateOrInsert(String[] commandParts, DataOutputStream outs) throws IOException {
        String key;
        String val;
        key = commandParts[1];
        val = commandParts[2];

        recoverEntry(key);
        datastore.put(key, val);

        respondSuccess(outs);
    }

    private void recoverEntry(String key) {
        if (tombstones.contains(key))
            tombstones.remove(key);
    }
}
