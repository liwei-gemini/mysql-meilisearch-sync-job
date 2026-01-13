package org.example;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

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
            // Determine index name
            String indexName = defaultIndexName;
            if (value.containsKey("_meili_index")) {
                indexName = value.getString("_meili_index");
                value.remove("_meili_index"); // Clean up before sending
            }

            // Extract the actual data payload (usually "after" for CDC)
            // But if we want to sink the whole event or just the data, depends on req.
            // Usually we sink the 'after' state for search.
            // Let's assume we sink the 'after' object if present, or the whole object if
            // not standard CDC.
            // However, the previous implementation sinks `value` which was a String.
            // CDC Debezium JSON usually has "before", "after", "source", "op".
            // We likely want to sink the "after" content + enriched fields.

            JSONObject docToSink = value.getJSONObject("after");
            if (docToSink == null) {
                // Determine if this is a delete or just not a CDC wrapping
                // If "op" is "d", we might want to delete from Meili, but standard sink just
                // adds.
                // For safety, if no "after", we skip or sink the whole thing if it's not
                // structured.
                // Let's fallback to sinking the whole value if "after" is missing.
                docToSink = value;
            }

            meiliSearchUtil.addDocuments(indexName, docToSink.toJSONString());
        } catch (Exception e) {
            System.err.println("Error writing to Meilisearch: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
