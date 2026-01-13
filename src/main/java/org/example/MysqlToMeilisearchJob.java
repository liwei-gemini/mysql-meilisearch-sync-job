package org.example;

// [关键] 使用 com.ververica 包 (CDC 2.x)
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Properties;

public class MysqlToMeilisearchJob {

    public static void main(String[] args) throws Exception {
        // 1. 加载配置 (注意加载的是 application-meilisearch.properties)
        ParameterTool envParams = loadEnvironmentConfig();
        ParameterTool taskParams = loadConfigFromResources("application-meilisearch.properties");
        ParameterTool mainParams = ParameterTool.fromArgs(args)
                .mergeWith(envParams)
                .mergeWith(taskParams);

        checkRequiredConfig(mainParams);

        // 2. 环境设置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(mainParams);
        env.setParallelism(mainParams.getInt("flink.parallelism", 1));
        env.enableCheckpointing(mainParams.getLong("flink.checkpoint.interval", 5000L));

        String[] tableList = Arrays.stream(mainParams.get("mysql.table-list").split(","))
                .map(String::trim)
                .toArray(String[]::new);

        // 3. [核心兼容逻辑] 准备 JDBC 属性
        Properties jdbcProps = new Properties();
        boolean enableMysql9Compat = mainParams.getBoolean("mysql.enable-mysql9-compat", false);
        if (enableMysql9Compat) {
            System.out.println(">>> [MeiliJob] 开启 MySQL 9.0+ 兼容模式 (挂载 Interceptor)");
            jdbcProps.setProperty("queryInterceptors", "org.example.MySQL9CompatibilityInterceptor");
        } else {
            System.out.println(">>> [MeiliJob] 使用默认 MySQL 模式 (5.7/8.0)");
        }

        // 4. 构建 Source (CDC 2.4.2 API)
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mainParams.get("mysql.hostname"))
                .port(mainParams.getInt("mysql.port"))
                .databaseList(mainParams.get("mysql.database"))
                .tableList(tableList)
                .username(mainParams.get("mysql.username"))
                .password(mainParams.get("mysql.password"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .includeSchemaChanges(false)
                .jdbcProperties(jdbcProps)
                .build();

        DataStreamSource<String> streamSource = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Meili Source");

        // 5. 转换 & 丰富数据 & Sink
        streamSource
                .map(new RecordEnricherMapFunction(
                        mainParams.get("mysql.hostname"),
                        mainParams.getInt("mysql.port"),
                        mainParams.get("mysql.database"), // Note: db list usually passed to source, here we need one
                                                          // for JDBC.
                                                          // Be careful if multiple DBs. Assuming mysql.database is
                                                          // single or main one.
                        mainParams.get("mysql.username"),
                        mainParams.get("mysql.password")))
                .addSink(new MeilisearchSink(
                        mainParams.get("meilisearch.host"),
                        mainParams.get("meilisearch.apiKey"),
                        mainParams.get("meilisearch.index")));

        env.execute("MySQL to Meilisearch Sync Job");
    }

    // --- 辅助方法 ---
    private static ParameterTool loadEnvironmentConfig() {
        try {
            ParameterTool mainProps = ParameterTool.fromPropertiesFile(
                    MysqlToMeilisearchJob.class.getResourceAsStream("/application.properties"));
            String activeProfile = mainProps.get("spring.profiles.active", "dev");
            return ParameterTool.fromPropertiesFile(
                    MysqlToMeilisearchJob.class.getResourceAsStream("/application-" + activeProfile + ".properties"));
        } catch (Exception e) {
            return ParameterTool.fromMap(new java.util.HashMap<>());
        }
    }

    private static ParameterTool loadConfigFromResources(String configFile) {
        try {
            return ParameterTool.fromPropertiesFile(
                    MysqlToMeilisearchJob.class.getResourceAsStream("/" + configFile));
        } catch (Exception e) {
            return ParameterTool.fromMap(new java.util.HashMap<>());
        }
    }

    private static void checkRequiredConfig(ParameterTool params) {
        String[] requiredKeys = { "mysql.hostname", "meilisearch.host", "meilisearch.apiKey" };
        for (String key : requiredKeys) {
            if (!params.has(key))
                throw new RuntimeException("Missing config: " + key);
        }
    }
}