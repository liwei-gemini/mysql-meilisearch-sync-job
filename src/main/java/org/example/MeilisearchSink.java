package org.example;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class MeilisearchSink extends RichSinkFunction<String> {

    private final String host;
    private final String apiKey;
    private final String indexName;
    private transient MeiliSearchUtil meiliSearchUtil;

    public MeilisearchSink(String host, String apiKey, String indexName) {
        this.host = host;
        this.apiKey = apiKey;
        this.indexName = indexName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.meiliSearchUtil = new MeiliSearchUtil(host, apiKey);
    }

    @Override
    public void invoke(String value, Context context) throws Exception {
        try {
            // value 已经是 JSON 字符串
            meiliSearchUtil.addDocuments(indexName, value);
        } catch (Exception e) {
            System.err.println("Error writing to Meilisearch: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
