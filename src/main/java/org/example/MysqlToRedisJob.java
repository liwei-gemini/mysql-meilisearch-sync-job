package org.example;

// ！！！注意：这里必须使用 org.apache.flink.cdc 开头的包，不要使用 com.ververica！！！
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.Properties;

public class MysqlToRedisJob {

    public static void main(String[] args) throws Exception {
        // 1. 加载配置 - 保持原有的配置加载逻辑
        ParameterTool envParams = loadEnvironmentConfig();
        ParameterTool taskParams = loadConfigFromResources("application-redis.properties");

        // 合并参数：命令行 > 任务配置 > 环境配置
        ParameterTool mainParams = ParameterTool.fromArgs(args)
                .mergeWith(envParams)
                .mergeWith(taskParams);

        // 检查必需配置
        checkRequiredConfig(mainParams);

        // 2. 获取并配置执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 注册全局参数
        env.getConfig().setGlobalJobParameters(mainParams);

        // 设置并行度
        env.setParallelism(mainParams.getInt("flink.parallelism", 1));

        // 开启 Checkpoint (CDC 任务强烈建议开启，用于断点续传)
        // 建议：生产环境应配合 RocksDBStateBackend 和 HDFS/S3 路径使用
        env.enableCheckpointing(mainParams.getLong("flink.checkpoint.interval", 5000L));

        // 处理 table-list
        String[] tableList = Arrays.stream(mainParams.get("mysql.table-list").split(","))
                .map(String::trim)
                .toArray(String[]::new);

        // 3. 准备 JDBC 属性 (核心修复点)
        Properties jdbcProps = new Properties();
        // 注册我们在另一个文件中定义的拦截器，用于替换 SHOW MASTER STATUS
        jdbcProps.setProperty("queryInterceptors", "org.example.MySQL9CompatibilityInterceptor");

        // 4. 配置 MySQL CDC Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(mainParams.get("mysql.hostname"))
                .port(mainParams.getInt("mysql.port"))
                .databaseList(mainParams.get("mysql.database")) // 监听的数据库
                .tableList(tableList)                           // 监听的表，格式 db.table
                .username(mainParams.get("mysql.username"))
                .password(mainParams.get("mysql.password"))
                .deserializer(new JsonDebeziumDeserializationSchema()) // 反序列化为 JSON 字符串
                .includeSchemaChanges(false) // 不包含 DDL 变更
                // ！！！关键：注入 JDBC 拦截器以兼容 MySQL 9.0！！！
                .jdbcProperties(jdbcProps)
                .build();

        // 5. 添加 Source 到环境
        DataStreamSource<String> streamSource = env.fromSource(
                mySqlSource,
                WatermarkStrategy.noWatermarks(),
                "MySQL CDC Source");

        // 6. 添加 Redis Sink
        // 注意：这里假设您的 RedisSink 类构造函数与之前提供的一致
        streamSource.addSink(new RedisSink(
                mainParams.get("redis.host"),
                mainParams.getInt("redis.port"),
                mainParams.get("redis.password"),
                mainParams.get("mysql.hostname"),
                mainParams.getInt("mysql.port"),
                mainParams.get("mysql.database"),
                mainParams.get("mysql.username"),
                mainParams.get("mysql.password")));

        // 7. 执行任务
        env.execute("MySQL 9.0 to Redis Sync Job");
    }

    /**
     * 加载环境配置文件（application-dev.properties 或 application-pro.properties）
     */
    private static ParameterTool loadEnvironmentConfig() {
        try {
            // 先加载主配置文件获取激活的环境
            ParameterTool mainProps = ParameterTool.fromPropertiesFile(
                    MysqlToRedisJob.class.getResourceAsStream("/application.properties"));

            String activeProfile = mainProps.get("spring.profiles.active", "dev");
            System.out.println(">>> Loading Profile: " + activeProfile);

            // 加载环境配置文件
            String profileConfigPath = "/application-" + activeProfile + ".properties";
            return ParameterTool.fromPropertiesFile(
                    MysqlToRedisJob.class.getResourceAsStream(profileConfigPath));
        } catch (Exception e) {
            System.err.println("Warning: Could not load environment configuration, using defaults. Error: " + e.getMessage());
            return ParameterTool.fromMap(new java.util.HashMap<>());
        }
    }

    /**
     * 检查必需配置是否存在
     */
    private static void checkRequiredConfig(ParameterTool params) {
        String[] requiredKeys = {
                "mysql.hostname", "mysql.port", "mysql.username", "mysql.password",
                "mysql.database", "mysql.table-list",
                "redis.host", "redis.port" // 建议也检查 Redis 配置
        };

        for (String key : requiredKeys) {
            if (!params.has(key)) {
                throw new RuntimeException("Missing required configuration: '" + key + "'");
            }
        }
    }

    /**
     * 加载指定的配置文件
     */
    private static ParameterTool loadConfigFromResources(String configFile) {
        try {
            return ParameterTool.fromPropertiesFile(
                    MysqlToRedisJob.class.getResourceAsStream("/" + configFile));
        } catch (Exception e) {
            System.err.println("Warning: " + configFile + " not found, using empty defaults.");
            return ParameterTool.fromMap(new java.util.HashMap<>());
        }
    }
}