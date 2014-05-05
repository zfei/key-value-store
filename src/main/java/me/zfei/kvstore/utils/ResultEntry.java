package me.zfei.kvstore.utils;

/**
 * Created by zfei on 5/5/14.
 */
public class ResultEntry {
    String key;
    String value;
    long timestamp;

    public ResultEntry(String key, String value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public long getTimestamp() {
        return timestamp;
    }
}