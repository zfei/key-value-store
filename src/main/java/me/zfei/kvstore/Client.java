package me.zfei.kvstore;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import me.zfei.kvstore.utils.CommandAction;
import me.zfei.kvstore.utils.Networker;
import me.zfei.kvstore.utils.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin.converter.util.CommandLineException;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

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

    public void startInputLoop() {
        Networker sender = new Networker();

        boolean exit = false;
        while (!exit) {
            System.out.print("kvstore cmd: ");
            Scanner sc = new Scanner(System.in, "UTF-8");
            while (sc.hasNext()) {
                String next = sc.nextLine();
                logger.debug("User input: " + next);

                // shutdown server on user exit
                if (next.equals("exit")) {
                    exit = true;
                    break;
                }

                try {
                    execute(next);
                } catch (CommandLineException e) {
                    logger.warn("Illegal command");
                }
            }
        }

        System.exit(0);
    }

    public int getTargetServerIndex(String key) {
        return key.hashCode() % serverConfigs.size();
    }

    private void checkArgNumCheck(int size, int safe) throws CommandLineException {
        if (size < safe)
            throw new CommandLineException("Illegal command");
    }

    public static CommandAction getActionType(String[] commandParts) {
        // Assumes clean input
        if (commandParts[0].equals("search-all"))
            return CommandAction.SEARCHALL;
        else
            return CommandAction.valueOf(commandParts[0].toUpperCase());
    }

    public void execute(String command) throws CommandLineException {
        String[] commandParts = command.split("\\s+");

        checkArgNumCheck(commandParts.length, 1);

        CommandAction actionType = getActionType(commandParts);
        switch (actionType) {
            case SEARCH:
                checkArgNumCheck(commandParts.length, 2);
                break;
            case SEARCHALL:
                break;
            case DELETE:
                checkArgNumCheck(commandParts.length, 2);
                break;
            case GET:
                checkArgNumCheck(commandParts.length, 3);
                break;
            case INSERT:
                checkArgNumCheck(commandParts.length, 4);
                break;
            case UPDATE:
                checkArgNumCheck(commandParts.length, 4);
                break;
        }
    }
}
