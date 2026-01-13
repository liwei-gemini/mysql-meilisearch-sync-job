package org.example;

import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

public class RecordEnricherMapFunction extends RichMapFunction<String, JSONObject> {

    private final String mysqlHost;
    private final int mysqlPort;
    private final String mysqlDatabase;
    private final String mysqlUser;
    private final String mysqlPassword;

    private transient Connection connection;

    public RecordEnricherMapFunction(String mysqlHost, int mysqlPort, String mysqlDatabase, String mysqlUser,
            String mysqlPassword) {
        this.mysqlHost = mysqlHost;
        this.mysqlPort = mysqlPort;
        this.mysqlDatabase = mysqlDatabase;
        this.mysqlUser = mysqlUser;
        this.mysqlPassword = mysqlPassword;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        String url = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&characterEncoding=utf-8",
                mysqlHost, mysqlPort, mysqlDatabase);
        connection = DriverManager.getConnection(url, mysqlUser, mysqlPassword);
        System.out.println("Enricher MySQL connection successful: " + url);
    }

    @Override
    public void close() throws Exception {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }

    @Override
    public JSONObject map(String value) throws Exception {
        JSONObject json = JSONObject.parseObject(value);
        JSONObject source = json.getJSONObject("source");
        if (source == null) {
            return json; // Should not happen for valid CDC events
        }

        String table = source.getString("table");

        // Handle bu_follow_up_plan enrichment
        if ("bu_follow_up_plan".equals(table)) {
            enrichBuFollowUpPlan(json);
        } else if ("bu_patient".equals(table)) {
            // Default behavior for other tables or explicit index mapping if needed
            // For now, let MeilisearchSink fallback to default index or handle as is
        }

        return json;
    }

    private void enrichBuFollowUpPlan(JSONObject json) {
        JSONObject after = json.getJSONObject("after");
        if (after == null)
            return; // Deletion or invalid state

        String patientId = after.getString("patient_id");
        if (patientId != null) {
            try {
                // Query bu_bedside
                String sql = "SELECT in_hospital_time, out_hospital_time FROM bu_bedside WHERE id = ?";
                try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                    stmt.setString(1, patientId);
                    try (ResultSet rs = stmt.executeQuery()) {
                        if (rs.next()) {
                            after.put("in_hospital_time", rs.getObject("in_hospital_time"));
                            after.put("out_hospital_time", rs.getObject("out_hospital_time"));
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Error enriching bu_follow_up_plan: " + e.getMessage());
                // Can decide to throw or just log. Logging allows stream to continue.
            }
        }

        // Tag with target index
        json.put("_meili_index", "buFollowUpPlan");
    }
}
