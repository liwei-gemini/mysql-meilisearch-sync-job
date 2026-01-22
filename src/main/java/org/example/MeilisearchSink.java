package org.example;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.security.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class MeilisearchSink extends RichSinkFunction<JSONObject> {

    private final String host;
    private final String apiKey;
    private final String defaultIndexName;
    private transient MeiliSearchUtil meiliSearchUtil;

    public MeilisearchSink(String host, String apiKey, String defaultIndexName) {
        this.host = host;
        this.apiKey = apiKey;
        this.defaultIndexName = defaultIndexName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.meiliSearchUtil = new MeiliSearchUtil(host, apiKey);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        try {
            // 1. 确定索引名称
            String indexName = defaultIndexName;
            if (value.containsKey("_meili_index")) {
                indexName = value.getString("_meili_index");
                value.remove("_meili_index");
            }

            String op = value.getString("op");
            JSONObject after = value.getJSONObject("after");
            JSONObject before = value.getJSONObject("before");

            // 2. 处理物理删除 (op = 'd')
            if ("d".equals(op)) {
                if (before != null && before.containsKey("id")) {
                    String id = before.getString("id");
                    System.out.println(">>> Deleting (Physical) doc " + id + " from index " + indexName);
                    meiliSearchUtil.deleteDocuments(indexName, Collections.singletonList(id));
                }
                return;
            }

            // 3. 处理逻辑删除 - 根据数据库名使用不同的删除条件
            JSONObject source = value.getJSONObject("source");
            if (source != null) {
                String database = source.getString("db");
                
                // bwjc数据库的表：使用useflag判断逻辑删除
                if ("bwjc".equals(database)) {
                    if (after != null && isBwjcRecordDeleted(after)) {
                        if (after.containsKey("id")) {
                            String id = after.getString("id");
                            System.out.println(">>> Deleting (Logical bwjc useflag=0) doc " + id + " from index " + indexName);
                            meiliSearchUtil.deleteDocuments(indexName, Collections.singletonList(id));
                        }
                        return;
                    }
                }
                // surveyking数据库的表：使用deleted判断逻辑删除
                else if ("surveyking".equals(database)) {
                    if (after != null && isSurveykingRecordDeleted(after)) {
                        if (after.containsKey("id")) {
                            String id = after.getString("id");
                            System.out.println(">>> Deleting (Logical surveyking deleted=1) doc " + id + " from index " + indexName);
                            meiliSearchUtil.deleteDocuments(indexName, Collections.singletonList(id));
                        }
                        return;
                    }
                }
            }

            // 4. 正常 新增/更新 (op = 'c', 'u', 'r')
            if (after != null) {
                // [新增] 将所有非字符串类型的值转换为字符串
                convertAllValuesToString(after);
                
                // 打印一下返回值，方便确认任务是否提交成功
                System.out.println(">>> Upsert request: " + after.toJSONString());
                String response = meiliSearchUtil.addDocuments(indexName, after.toJSONString());
                 System.out.println(">>> Upsert response: " + response);
            } else {
                // 如果是其他情况且 after 为空，可能不需要处理，或者是异常数据
                System.err.println(">>> Warning: Received CDC event with null 'after' block, op: " + op);
            }

        } catch (Exception e) {
            System.err.println("Error writing to Meilisearch: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // [新增] bwjc数据库逻辑删除判断：useflag = 0 表示删除
    private boolean isBwjcRecordDeleted(JSONObject after) {
        if (after.containsKey("useflag")) {
            Object useflag = after.get("useflag");
            if (useflag instanceof String) {
                return "0".equals(useflag);
            } else if (useflag instanceof Number) {
                return ((Number) useflag).intValue() == 0;
            }
        }
        return false;
    }

    // [新增] surveyking数据库逻辑删除判断：deleted = 1 表示删除
    private boolean isSurveykingRecordDeleted(JSONObject after) {
        if (after.containsKey("deleted")) {
            Object deleted = after.get("deleted");
            if (deleted instanceof String) {
                return "1".equals(deleted);
            } else if (deleted instanceof Number) {
                return ((Number) deleted).intValue() == 1;
            }
        }
        return false;
    }

    // [修改] 将所有非字符串类型的值转换为字符串，特别处理日期类型，但跳过列表类型
    private void convertAllValuesToString(JSONObject jsonObject) {
        if (jsonObject == null) return;
        
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        
        for (String key : jsonObject.keySet()) {
            Object value = jsonObject.get(key);
            if (value != null) {
                // [修改] 跳过列表类型的转换，保持为JSON数组
                if (value instanceof List) {
                    continue; // 保持列表类型不变
                }
                
                if (!(value instanceof String)) {
                    // 处理日期类型
                    if (value instanceof Date) {
                        // java.util.Date类型
                        jsonObject.put(key, dateFormat.format((Date) value));
                    } else if (value instanceof Timestamp) {
                        // java.sql.Timestamp类型
                        jsonObject.put(key, dateFormat.format((Timestamp) value));
                    } else if (value instanceof java.sql.Date) {
                        // java.sql.Date类型，需要转换为java.util.Date进行格式化
                        java.sql.Date sqlDate = (java.sql.Date) value;
                        jsonObject.put(key, dateFormat.format(new Date(sqlDate.getTime())));
                    } else {
                        // 其他非字符串类型
                        jsonObject.put(key, value.toString());
                    }
                }
                // 如果是字符串类型，保持原样
            } else {
                // 如果值为null，保持null
                jsonObject.put(key, null);
            }
        }
    }
}