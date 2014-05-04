package me.zfei.kvstore.utils;

/**
 * Created by zfei on 5/4/14.
 */
public class ServerConfig {
    String host;
    int port;
    int delay;

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public int getDelay() {
        return delay;
    }

    @Override
    public String toString() {
        return host + ":" + port + ", " + delay;
    }
}