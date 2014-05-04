package me.zfei.kvstore;

import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;
import me.zfei.kvstore.utils.Networker;
import me.zfei.kvstore.utils.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin.converter.util.CommandLineException;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by zfei on 5/3/14.
 */
public class Server {
    private static Logger logger = LoggerFactory.getLogger(Server.class);
    private Networker networker;
    private int port;
    private static List<ServerConfig> serverConfigs;

    private Map<String, String> dataStore = new HashMap<String, String>();

    public Server(int port) {
        networker = new Networker();
        this.port = port;
    }

    public int getTargetServerIndex(String key) {
        return key.hashCode() % serverConfigs.size();
    }

    public void start() {
        networker.startListener(port);
    }

    public void shutdown() {
        System.exit(0);
    }

    public void execute(String command) throws CommandLineException {
        String[] commandParts = command.split(" ");

        if (commandParts.length < 1)
            throw new CommandLineException("Illegal command");

        String part0 = commandParts[0];
        if (part0.equals("delete")) {

        } else if (part0.equals("get")) {

        } else if (part0.equals("insert")) {

        } else if (part0.equals("update")) {

        } else if (part0.equals("search-all")) {

        } else if (part0.equals("search")) {

        }
    }

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
                } else {
                    // skip irrelevant names
                    reader.skipValue();
                }
            }
        } catch (IOException e) {
            logger.error("Please provide correct config file as ./conf/settings.json");
            System.exit(1);
        }
    }

    public static void main(String[] args) {
        // check param
        if (args.length != 1) {
            System.out.println("Usage: java -jar Server.jar SERVER_INDEX");
            System.exit(1);
        }

        loadConfigFile();

        if (serverConfigs == null || serverConfigs.size() == 0) {
            logger.error("Please provide correct config file as ./conf/settings.json");
            System.exit(1);
        }

        // check if server index is legal
        int serverIndex = Integer.parseInt(args[0]);
        int numServers = serverConfigs.size();
        if (serverIndex < 0 || serverIndex >= numServers) {
            logger.error(String.format("Illegal server index %d, expected [0, %d]", serverIndex, numServers));
            System.exit(1);
        }

        Server server = new Server(serverConfigs.get(serverIndex).getPort());
        server.start();

        Networker client = new Networker();

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
                    server.execute(next);
                } catch (CommandLineException e) {
                    logger.warn("Illegal command");
                }
            }
        }

        server.shutdown();
    }
}
