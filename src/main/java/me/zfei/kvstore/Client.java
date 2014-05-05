package me.zfei.kvstore;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import me.zfei.kvstore.utils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zfei on 5/4/14.
 */
public class Client {

    private static Logger logger = LoggerFactory.getLogger(Client.class);
    public static List<ServerConfig> serverConfigs;
    private static int numReplicas = 3; // keep 3 replicas by default

    public static void loadConfigFile() {
        try {
            JsonReader reader = new JsonReader(new FileReader("conf/settings.json"));
            reader.beginObject();
            while (reader.hasNext()) {
                String name = reader.nextName();
                if (name.equals("servers")) {
                    // init server configs
                    serverConfigs = new ArrayList<ServerConfig>();

                    // read array
                    reader.beginArray();
                    while (reader.hasNext()) {
                        ServerConfig config = new Gson().fromJson(reader, ServerConfig.class);
                        serverConfigs.add(config);
                        logger.debug(String.format("Added server config: %s", config));
                    }

                    reader.endArray();
                } else if (name.equals("numReplicas")) {
                    numReplicas = reader.nextInt();
                } else {
                    // skip irrelevant names
                    reader.skipValue();
                }
            }
        } catch (IOException e) {
            logger.error("Please provide correct config file as ./conf/settings.json");
            System.exit(1);
        }

        if (serverConfigs == null || serverConfigs.size() == 0) {
            logger.error("Please provide correct config file as ./conf/settings.json");
            System.exit(1);
        }
    }

    public void startInputLoop() throws IOException {
        Networker sender = new Networker();

        DataInputStream input = new DataInputStream(System.in);
        BufferedReader d = new BufferedReader(new InputStreamReader(input));
        while (true) {
            System.out.print("kvstore cmd: ");
            String next = d.readLine();
            logger.debug("User input: " + next);

            // shutdown server on user exit
            if (next.equals("exit")) {
                break;
            }

            try {
                execute(sender, next);
            } catch (IllegalArgumentException e) {
                logger.warn("Illegal command");
            }
        }

        System.exit(0);
    }

    public int getTargetServerIndex(String key) {
        return key.hashCode() % serverConfigs.size();
    }

    public int[] getReplicaIndices(String key) {
        int target = getTargetServerIndex(key);
        int[] indices = new int[numReplicas];
        for (int i = 0; i < numReplicas; i++) {
            indices[i] = (target + i) % serverConfigs.size();
        }

        return indices;
    }

    private void checkArgNumCheck(int size, int safe) throws IllegalArgumentException {
        if (size < safe) {
            logger.warn(String.format("Got %d args, expected %d args", size, safe));
            throw new IllegalArgumentException("Wrong number of arguments");
        }
    }

    public static CommandAction getActionType(String[] commandParts) throws IllegalArgumentException {
        // Assumes clean input
        if (commandParts[0].equals("show-all"))
            return CommandAction.SHOWALL;
        else
            return CommandAction.valueOf(commandParts[0].toUpperCase());
    }

    public void execute(final Networker sender, final String command) throws IllegalArgumentException {
        String[] commandParts = command.trim().split("\\s+");

        checkArgNumCheck(commandParts.length, 1);

        // reject illegal commands
        CommandAction actionType;
        try {
            actionType = getActionType(commandParts);
        } catch (IllegalArgumentException e) {
            logger.warn("Illegal action");
            return;
        }

        switch (actionType) {
            case SEARCH:
                checkArgNumCheck(commandParts.length, 2);

                multicastCommand(actionType, sender, command, commandParts[1], "ALL", false);
                break;
            case SHOWALL:
                if (Standalone.serverIndex != -1)
                    sender.unicastSend(serverConfigs.get(Standalone.serverIndex), command, 0, System.currentTimeMillis());
                break;
            case DELETE:
                checkArgNumCheck(commandParts.length, 2);

                multicastCommand(actionType, sender, command, commandParts[1], "ALL", true);
                break;
            case GET:
                checkArgNumCheck(commandParts.length, 3);

                multicastCommand(actionType, sender, command, commandParts[1], commandParts[2], true);
                break;
            case INSERT:
                checkArgNumCheck(commandParts.length, 4);

                multicastCommand(actionType, sender, command, commandParts[1], commandParts[3], true);
                break;
            case UPDATE:
                checkArgNumCheck(commandParts.length, 4);

                multicastCommand(actionType, sender, command, commandParts[1], commandParts[3], true);
                break;
        }
    }

    private void multicastCommand(final CommandAction actionType, final Networker sender, final String command, String key, String consistency, final boolean enableDelay) {
        ConsistencyLevel cl;
        try {
            cl = ConsistencyLevel.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Please enter legal consistency level (ONE/ALL)");
            return;
        }

        final String[] results = new String[numReplicas];
        final int[] replicaIndices = getReplicaIndices(key);
        for (int i = 0; i < replicaIndices.length; i++) {
            final int serverIndex = replicaIndices[i];
            final int finalI = i;
            Thread sendingThread = new Thread() {
                @Override
                public void run() {
                    int averageDelay = 0;
                    if (enableDelay && Standalone.serverIndex != -1)
                        averageDelay = serverConfigs.get(Standalone.serverIndex).getDelays()[serverIndex];

                    long timestamp = System.currentTimeMillis();
                    results[finalI] = sender.unicastSend(serverConfigs.get(serverIndex), command, averageDelay, timestamp);
                }
            };
            sendingThread.start();
        }

        switch (cl) {
            case ONE:
                boolean exit = false;
                while (!exit) {
                    for (String str : results) {
                        if (str != null) {
                            logger.info(str);
                            exit = true;
                        }
                    }
                }
                break;
            case ALL:
                exit = false;
                while (!exit) {
                    exit = true;
                    for (String str : results) {
                        if (str == null)
                            exit = false;
                    }
                }

                logger.info(results[0]);
                break;
            default:
                break;
        }

        if (actionType == CommandAction.GET)
            issueReadRepair(sender, replicaIndices, results);
    }

    public void issueReadRepair(final Networker sender, final int[] replicaIndices, final String[] results) {
        final Thread readRepairThread = new Thread() {
            @Override
            public void run() {
                QueryResult[] qr = new QueryResult[numReplicas];

                boolean exit = false;
                while (!exit) {
                    exit = true;

                    for (int i = 0; i < replicaIndices.length; i++) {
                        if (qr[i] == null && results[i] != null)
                            qr[i] = new Gson().fromJson(results[i], QueryResult.class);

                        if (qr[i] == null)
                            exit = false;
                    }
                }

                boolean inconsistent = false;
                for (int i = 1; i < qr.length; i++) {
                    if (qr[i].isSuccess() != qr[i - 1].isSuccess())
                        inconsistent = true;
                    else if (qr[i].isSuccess()) {
                        String currentVal = qr[i].getResult().get(0).getValue();
                        String previousVal = qr[i - 1].getResult().get(0).getValue();
                        if (!currentVal.equals(previousVal))
                            inconsistent = true;
                    }
                }

                if (!inconsistent)
                    return;

                logger.debug("Inconsistency detected");

                long latestTimestamp = -1;
                int correctServerIndex = -1;
                boolean shouldDelete = false;
                String key = "", value = "";
                for (int i = 0; i < qr.length; i++) {
                    long timestamp = -1;
                    List<ResultEntry> reList = qr[i].getResult();
                    if (reList == null) {
                        if (!qr[i].getReason().startsWith("Deleted"))
                            continue;
                        else {
                            timestamp = Long.parseLong(qr[i].getReason().substring(qr[i].getReason().indexOf('@') + 1));
                        }
                    } else {
                        ResultEntry re = reList.get(0);
                        timestamp = re.getTimestamp();

                        key = re.getKey();
                        value = re.getValue();
                    }

                    if (timestamp > latestTimestamp) {
                        correctServerIndex = replicaIndices[i];
                        latestTimestamp = timestamp;

                        if (reList == null)
                            shouldDelete = true;
                    }
                }

                final String message;
                if (shouldDelete)
                    message = String.format("DELETE %s", key);
                else
                    message = String.format("UPDATE %s %s ALL", key, value);

                for (int i = 0; i < replicaIndices.length; i++) {
                    final int serverIndex = replicaIndices[i];
                    if (serverIndex == correctServerIndex)
                        continue;

                    int averageDelay = 0;
                    if (Standalone.serverIndex != -1)
                        averageDelay = serverConfigs.get(Standalone.serverIndex).getDelays()[serverIndex];

                    final int finalAverageDelay = averageDelay;
                    final long finalLatestTimestamp = latestTimestamp;
                    Thread sendingThread = new Thread() {
                        @Override
                        public void run() {
                            sender.unicastSend(serverConfigs.get(serverIndex), message, finalAverageDelay, finalLatestTimestamp);
                        }
                    };
                    sendingThread.start();
                }
            }
        };

        readRepairThread.start();
    }
}
