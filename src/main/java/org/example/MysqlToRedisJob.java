package org.example;

// 1. 换回 com.ververica 包
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Properties;

public class MysqlToRedisJob {

    public static void main(String[] args) throws Exception {
        // ... (配置加载部分不变) ...
        ParameterTool envParams = loadEnvironmentConfig();
        ParameterTool taskParams = loadConfigFromResources("application-redis.properties");
        ParameterTool mainParams = ParameterTool.fromArgs(args).mergeWith(envParams).mergeWith(taskParams);
        checkRequiredConfig(mainParams);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(mainParams);
        env.setParallelism(mainParams.getInt("flink.parallelism", 1));
        env.enableCheckpointing(mainParams.getLong("flink.checkpoint.interval", 5000L));

        String[] tableList = Arrays.stream(mainParams.get("mysql.table-list").split(","))
                .map(String::trim)
                .toArray(String[]::new);

        // 修复：支持多个数据库配置
        String databaseConfig = mainParams.get("mysql.database");
        String[] databaseList;
        if (databaseConfig.contains(",")) {
            databaseList = Arrays.stream(databaseConfig.split(","))
                    .map(String::trim)
                    .toArray(String[]::new);
        } else {
            databaseList = new String[]{databaseConfig};
        }

        // 2. 准备 JDBC 属性 (CDC 2.x 也支持 jdbcProperties)
        Properties jdbcProps = new Properties();
        boolean enableMysql9Compat = mainParams.getBoolean("mysql.enable-mysql9-compat", false);
        if (enableMysql9Compat) {
            System.out.println(">>> [RedisJob] 开启 MySQL 9.0+ 兼容模式");
            jdbcProps.setProperty("queryInterceptors", "org.example.MySQL9CompatibilityInterceptor");
        }

        // 3. 构建 Source (CDC 2.x 写法)
        // 注意：这里不需要 MySqlSourceBuilder 变量，直接 MySqlSource.<String>builder() 即可
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mainParams.get("mysql.hostname"))
                .port(mainParams.getInt("mysql.port"))
                .databaseList(databaseList)  // 修改：使用数据库数组而不是字符串
                .tableList(tableList)
                .username(mainParams.get("mysql.username"))
                .password(mainParams.get("mysql.password"))
                .deserializer(new JsonDebeziumDeserializationSchema())
                .includeSchemaChanges(false)
                .jdbcProperties(jdbcProps) // 2.4.2 支持此方法
                .build();

        DataStreamSource<String> streamSource = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source");

        // Sink 部分不变
        streamSource.addSink(new RedisSink(
                mainParams.get("redis.host"),
                mainParams.getInt("redis.port"),
                mainParams.get("redis.password"),
                mainParams.get("mysql.hostname"),
                mainParams.getInt("mysql.port"),
                mainParams.get("mysql.database"),
                mainParams.get("mysql.username"),
                mainParams.get("mysql.password")));

        env.execute("MySQL to Redis Sync Job");
    }

    // ... (辅助方法保持不变) ...
    private static ParameterTool loadEnvironmentConfig() {
        try {
            ParameterTool mainProps = ParameterTool.fromPropertiesFile(
                    MysqlToRedisJob.class.getResourceAsStream("/application.properties"));
            String activeProfile = mainProps.get("spring.profiles.active", "dev");
            return ParameterTool.fromPropertiesFile(
                    MysqlToRedisJob.class.getResourceAsStream("/application-" + activeProfile + ".properties"));
        } catch (Exception e) { return ParameterTool.fromMap(new java.util.HashMap<>()); }
    }

    private static ParameterTool loadConfigFromResources(String configFile) {
        try { return ParameterTool.fromPropertiesFile(MysqlToRedisJob.class.getResourceAsStream("/" + configFile)); }
        catch (Exception e) { return ParameterTool.fromMap(new java.util.HashMap<>()); }
    }

    private static void checkRequiredConfig(ParameterTool params) {
        if (!params.has("mysql.hostname")) throw new RuntimeException("Missing config");
    }
}