package me.zfei.kvstore.utils;

import java.util.List;

/**
 * Created by zfei on 5/5/14.
 */
public class QueryResult {
    boolean success;
    String reason;
    List<ResultEntry> result;

    public QueryResult(boolean success) {
        this.success = success;
    }

    public QueryResult(boolean success, String reason) {
        this.success = success;
        this.reason = reason;
    }

    public QueryResult(boolean success, List<ResultEntry> result) {
        this.success = success;
        this.result = result;
    }
}
