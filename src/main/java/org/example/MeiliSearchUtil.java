package org.example;

import cn.hutool.http.HttpRequest;
import cn.hutool.http.HttpResponse;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONUtil;
import java.util.Collections;
import java.util.List;

/**
 * MeiliSearch 工具类 - 纯 HTTP 实现
 * 避免官方 SDK 带来的 JDK 版本冲突和 OkHttp 冲突
 */
public class MeiliSearchUtil {

    private final String host;
    private final String apiKey;

    public MeiliSearchUtil(String host, String apiKey) {
        this.host = host.endsWith("/") ? host.substring(0, host.length() - 1) : host;
        this.apiKey = apiKey;
    }

    /**
     * 批量添加文档
     *
     * @param indexName 索引名称
     * @param documents 文档列表 (可以是 JSON 字符串)
     * @return 响应结果
     */
    public String addDocuments(String indexName, String documents) {
        String url = host + "/indexes/" + indexName + "/documents";

        // 确保格式是数组
        String finalJson = documents.trim();
        if (finalJson.startsWith("{")) {
            finalJson = "[" + finalJson + "]";
        }

        try (HttpResponse response = HttpRequest.post(url)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .body(finalJson)
                .execute()) {

            if (response.isOk()) {
                return response.body();
            } else {
                throw new RuntimeException("Meilisearch error: " + response.getStatus() + " - " + response.body());
            }
        }
    }

    /**
     * 批量添加文档 (List 对象)
     */
    public <T> String addDocuments(String indexName, List<T> documents) {
        return addDocuments(indexName, JSONUtil.toJsonStr(documents));
    }
    /**
     * 批量删除文档
     *API: POST /indexes/{index_uid}/documents/delete-batch
     */
    public String deleteDocuments(String indexName, List<String> ids) {
        String url = host + "/indexes/" + indexName + "/documents/delete-batch";
        String body = JSONUtil.toJsonStr(ids);

        try (HttpResponse response = HttpRequest.post(url)
                .header("Authorization", "Bearer " + apiKey)
                .header("Content-Type", "application/json")
                .body(body)
                .execute()) {

            if (response.isOk()) {
                return response.body();
            } else {
                throw new RuntimeException("Meilisearch delete error: " + response.getStatus() + " - " + response.body());
            }
        }
    }
}