package me.zfei.kvstore;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import me.zfei.kvstore.utils.CommandAction;
import me.zfei.kvstore.utils.ConsistencyLevel;
import me.zfei.kvstore.utils.Networker;
import me.zfei.kvstore.utils.ServerConfig;
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
        if (size < safe)
            throw new IllegalArgumentException("Illegal command");
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
            logger.warn("Please enter legal commands");
            return;
        }

        switch (actionType) {
            case SEARCH:
                checkArgNumCheck(commandParts.length, 2);

                multicastCommand(sender, command, commandParts[1], "ALL");
                break;
            case SHOWALL:
                if (Standalone.serverIndex != -1)
                    sender.unicastSend(serverConfigs.get(Standalone.serverIndex), command);
                break;
            case DELETE:
                checkArgNumCheck(commandParts.length, 2);

                multicastCommand(sender, command, commandParts[1], "ALL");
                break;
            case GET:
                checkArgNumCheck(commandParts.length, 3);

                multicastCommand(sender, command, commandParts[1], commandParts[2]);
                break;
            case INSERT:
                checkArgNumCheck(commandParts.length, 4);

                multicastCommand(sender, command, commandParts[1], commandParts[3]);
                break;
            case UPDATE:
                checkArgNumCheck(commandParts.length, 4);

                multicastCommand(sender, command, commandParts[1], commandParts[3]);
                break;
        }
    }

    private void multicastCommand(final Networker sender, final String command, String key, String consistency) {
        ConsistencyLevel cl;
        try {
            cl = ConsistencyLevel.valueOf(consistency.toUpperCase());
        } catch (IllegalArgumentException e) {
            logger.warn("Please enter legal consistency level (ONE/ALL)");
            return;
        }

        final String[] results = new String[numReplicas];
        int[] replicaIndices = getReplicaIndices(key);
        for (int i = 0; i < replicaIndices.length; i++) {
            final int serverIndex = replicaIndices[i];
            final int finalI = i;
            Thread sendingThread = new Thread() {
                @Override
                public void run() {
                    results[finalI] = sender.unicastSend(serverConfigs.get(serverIndex), command);
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
    }
}
