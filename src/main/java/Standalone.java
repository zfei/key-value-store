import me.zfei.kvstore.Client;
import me.zfei.kvstore.Server;
import me.zfei.kvstore.utils.ServerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by zfei on 5/4/14.
 */
public class Standalone {

    private static Logger logger = LoggerFactory.getLogger(Standalone.class);


    public static void main(String[] args) {
        // check param
        if (args.length != 1) {
            System.out.println("Usage: java -jar Server.jar SERVER_INDEX");
            System.exit(1);
        }

        Client client = new Client();
        client.loadConfigFile();

        // check if server index is legal
        List<ServerConfig> serverConfigs = Client.serverConfigs;
        int serverIndex = Integer.parseInt(args[0]);
        int numServers = serverConfigs.size();
        if (serverIndex < 0 || serverIndex >= numServers) {
            logger.error(String.format("Illegal server index %d, expected [0, %d]", serverIndex, numServers));
            System.exit(1);
        }

        Server server = new Server(serverConfigs.get(serverIndex).getPort());
        server.start();

        client.startInputLoop();

        server.shutdown();
    }
}
