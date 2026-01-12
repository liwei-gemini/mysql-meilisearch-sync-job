package org.example;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.ConcurrentHashMap;

public class RedisSink extends RichSinkFunction<String> {

    private transient Jedis jedis;
    private transient Connection connection;

    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;

    // MySQL Info
    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUser;
    private final String mysqlPassword;

    // Cache: dict_id -> dict_code
    private transient ConcurrentHashMap<String, String> dictCodeCache;

    public RedisSink(String redisHost, int redisPort, String redisPassword,
            String mysqlHost, int mysqlPort, String mysqlDatabase,
            String mysqlUser, String mysqlPassword) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        try {
            // Redis Connection
            jedis = new Jedis(redisHost, redisPort, 5000);
            if (redisPassword != null && !redisPassword.isEmpty()) {
                jedis.auth(redisPassword);
            }
            jedis.ping();
            System.out.println("Redis connection successful: " + redisHost + ":" + redisPort);

            // MySQL Connection
            Class.forName("com.mysql.cj.jdbc.Driver");
            String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&characterEncoding=utf-8",
                    mysqlHost, mysqlPort, mysqlDatabase);
            connection = DriverManager.getConnection(url, mysqlUser, mysqlPassword);
            System.out.println("MySQL connection successful: " + url);

            // Init Cache
            dictCodeCache = new ConcurrentHashMap<>();
        } catch (Exception e) {
            System.err.println("Connection failed: " + e.getMessage());
            throw e;
        }
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
        System.out.println("Resources closed.");
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            JSONObject json = JSONObject.parseObject(value);
            String op = json.getString("op");
            JSONObject after = json.getJSONObject("after");
            JSONObject before = json.getJSONObject("before");
            JSONObject source = json.getJSONObject("source");

            String database = source != null ? source.getString("db") : null;
            String table = source != null ? source.getString("table") : null;

            if (database == null || table == null) {
                return;
            }

            String tableName = extractTableName(table);
            if ("ba_datadict".equals(tableName)) {
                handleDataDict(op, before, after);
            } else if ("ba_datadict_detail".equals(tableName)) {
                handleDataDictDetail(op, before, after);
            } else {
                handleGenericData(op, database, tableName, before, after);
            }
        } catch (Exception e) {
            System.err.println("Error processing record: " + value);
            e.printStackTrace();
            throw e; // Fail fast
        }
    }

    private String extractTableName(String fullTableName) {
        if (fullTableName == null)
            return null;
        int dotIndex = fullTableName.lastIndexOf('.');
        return dotIndex >= 0 ? fullTableName.substring(dotIndex + 1) : fullTableName;
    }

    private void handleDataDict(String op, JSONObject before, JSONObject after) {
        if ("c".equals(op) || "r".equals(op)) {
            if (after != null) {
                updateDictCache(after);
            }
        } else if ("u".equals(op)) {
            if (after != null && before != null) {
                String oldCode = before.getString("dict_code");
                String newCode = after.getString("dict_code");

                if (oldCode != null && newCode != null && !oldCode.equals(newCode)) {
                    System.out.println("Dict Code Change: " + oldCode + " -> " + newCode);
                    String oldKey = "dict:" + oldCode;
                    String newKey = "dict:" + newCode;
                    if (jedis.exists(oldKey)) {
                        jedis.renamenx(oldKey, newKey);
                    }
                }
                updateDictCache(after);
            }
        } else if ("d".equals(op)) {
            if (before != null) {
                String id = before.getString("dict_id");
                String code = before.getString("dict_code");
                if (id != null) {
                    dictCodeCache.remove(id);
                }
                if (code != null) {
                    jedis.del("dict:" + code);
                }
            }
        }
    }

    private void updateDictCache(JSONObject record) {
        String id = record.getString("dict_id");
        String code = record.getString("dict_code");
        if (id != null && code != null) {
            dictCodeCache.put(id, code);
        }
    }

    private void handleDataDictDetail(String op, JSONObject before, JSONObject after) throws Exception {
        if ("c".equals(op) || "r".equals(op)) {
            if (after != null) {
                upsertDetail(after);
            }
        } else if ("u".equals(op)) {
            if (after != null && before != null) {
                String oldValue = before.getString("detail_value");
                String newValue = after.getString("detail_value");
                String dictId = after.getString("dict_id");

                if (oldValue != null && newValue != null && !oldValue.equals(newValue)) {
                    String dictCode = getDictCode(dictId);
                    if (dictCode != null) {
                        jedis.hdel("dict:" + dictCode, oldValue);
                    }
                }
                upsertDetail(after);
            }
        } else if ("d".equals(op)) {
            if (before != null) {
                String dictId = before.getString("dict_id");
                String value = before.getString("detail_value");
                if (dictId != null && value != null) {
                    String dictCode = getDictCode(dictId);
                    if (dictCode != null) {
                        jedis.hdel("dict:" + dictCode, value);
                    }
                }
            }
        }
    }

    private void upsertDetail(JSONObject detail) throws Exception {
        String dictId = detail.getString("dict_id");
        String value = detail.getString("detail_value");
        String name = detail.getString("detail_name");

        if (dictId != null && value != null && name != null) {
            String dictCode = getDictCode(dictId);
            if (dictCode != null) {
                String redisKey = "dict:" + dictCode;
                jedis.hset(redisKey, value, name);
                System.out.println("Detail synced: " + redisKey + " -> " + value + "=" + name);
            } else {
                System.err.println("Warning: Could not resolve dict_code for dict_id=" + dictId);
            }
        }
    }

    private String getDictCode(String dictId) throws Exception {
        // 1. Check Cache
        String code = dictCodeCache.get(dictId);
        if (code != null)
            return code;

        // 2. Query MySQL
        String sql = "SELECT dict_code FROM ba_datadict WHERE dict_id = ?";
        try (PreparedStatement stmt = connection.prepareStatement(sql)) {
            stmt.setString(1, dictId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    code = rs.getString("dict_code");
                    if (code != null) {
                        dictCodeCache.put(dictId, code);
                        return code;
                    }
                }
            }
        }
        return null;
    }

    private void handleGenericData(String op, String database, String table, JSONObject before, JSONObject after) {
        if ("c".equals(op) || "r".equals(op) || "u".equals(op)) {
            if (after != null) {
                String id = after.getString("id");
                if (id != null) {
                    String redisKey = database + ":" + table + ":" + id;
                    jedis.set(redisKey, after.toJSONString());
                }
            }
        } else if ("d".equals(op)) {
            if (before != null) {
                String id = before.getString("id");
                if (id != null) {
                    String redisKey = database + ":" + table + ":" + id;
                    jedis.del(redisKey);
                }
            }
        }
    }
}