package org.example;

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
        // 1. 加载配置
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

        // 3. JDBC 属性
        Properties jdbcProps = new Properties();
        if (mainParams.getBoolean("mysql.enable-mysql9-compat", false)) {
            jdbcProps.setProperty("queryInterceptors", "org.example.MySQL9CompatibilityInterceptor");
        }

        // 4. 构建 Source
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

        // [新增] 读取新的索引配置，提供默认值防止报错
        String indexPatient = mainParams.get("meilisearch.index.patient", "buPatients");
        String indexFollowup = mainParams.get("meilisearch.index.followup", "buFollowUpPlan");
        String indexBedside = mainParams.get("meilisearch.index.z", "buBedside");
        String indexPatientOrg = mainParams.get("meilisearch.index.patientOrg", "buPatientOrg");
        String indexSysUser = mainParams.get("meilisearch.index.sysUser", "sysUser");
        String indexSysOrg = mainParams.get("meilisearch.index.sysOrg", "sysOrg");
        
        // [新增] 读取Redis配置
        String redisHost = mainParams.get("redis.host");
        int redisPort = mainParams.getInt("redis.port", 6379);
        String redisPassword = mainParams.get("redis.password", "");
        
        // 5. 转换 & Sink
        streamSource
                .flatMap(new RecordEnricherFlatMapFunction(
                        mainParams.get("mysql.hostname"),
                        mainParams.getInt("mysql.port"),
                        mainParams.get("mysql.database"),
                        mainParams.get("mysql.username"),
                        mainParams.get("mysql.password"),
                        redisHost, // [新增] Redis主机
                        redisPort, // [新增] Redis端口
                        redisPassword, // [新增] Redis密码
                        indexPatient, // [新增] 传入患者索引名
                        indexFollowup, // [新增] 传入随访索引名
                        indexBedside, // [新增] 传入床位索引名
                        indexPatientOrg, // [新增] 传入机构索引名
                        indexSysUser, // [新增] 传入用户索引名
                        indexSysOrg // [新增] 传入组织索引名
                ))
                .addSink(new MeilisearchSink(
                        mainParams.get("meilisearch.host"),
                        mainParams.get("meilisearch.apiKey"),
                        indexPatient // 默认索引，实际上会由 MapFunction 覆盖
                ));

        env.execute("MySQL to Meilisearch Sync Job");
    }

    // ... (辅助方法保持不变 loadEnvironmentConfig, loadConfigFromResources,
    // checkRequiredConfig)
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