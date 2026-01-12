package org.example;

import com.mysql.cj.MysqlConnection;
import com.mysql.cj.Query;
import com.mysql.cj.interceptors.QueryInterceptor;
import com.mysql.cj.log.Log;
import com.mysql.cj.protocol.Resultset;
import com.mysql.cj.protocol.ServerSession;

import java.sql.SQLException;
import java.util.Properties;
import java.util.function.Supplier;

public class MySQL9CompatibilityInterceptor implements QueryInterceptor {

    private MysqlConnection connection;

    @Override
    public QueryInterceptor init(MysqlConnection conn, Properties props, Log log) {
        this.connection = conn;
        return this;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Resultset> T preProcess(Supplier<String> sql, Query interceptedQuery) {
        String query = sql.get();

        // 1. 增强匹配逻辑：忽略大小写，使用 contains 防止只有部分匹配
        if (query != null && query.toUpperCase().contains("SHOW MASTER STATUS")) {
            System.out.println(">>> [Mysql9Interceptor] 拦截到 'SHOW MASTER STATUS'，准备替换...");

            try {
                // 2. 获取 JDBC 连接
                if (this.connection instanceof java.sql.Connection) {
                    java.sql.Connection jdbcConn = (java.sql.Connection) this.connection;

                    // 3. 执行新命令 MySQL 9.0+
                    System.out.println(">>> [Mysql9Interceptor] 正在执行 'SHOW BINARY LOG STATUS'...");
                    java.sql.Statement stmt = jdbcConn.createStatement();
                    java.sql.ResultSet rs = stmt.executeQuery("SHOW BINARY LOG STATUS");

                    // 4. 强制转换结果
                    // MySQL Connector/J 的 ResultSetImpl 实现了 Resultset 接口，所以可以直接强转
                    // 如果这里报错，说明驱动版本差异，但通常 8.0.x 驱动是兼容的
                    return (T) rs;
                } else {
                    System.err.println(">>> [Mysql9Interceptor] 错误: Connection 不是 java.sql.Connection 实例，类型为: " +
                            (this.connection == null ? "null" : this.connection.getClass().getName()));
                }
            } catch (SQLException e) {
                System.err.println(">>> [Mysql9Interceptor] 执行替换 SQL 失败:");
                e.printStackTrace();
                // 抛出运行时异常让 Flink 任务显式失败，而不是静默忽略
                throw new RuntimeException("Failed to execute SHOW BINARY LOG STATUS in interceptor", e);
            }
        }

        // 返回 null 表示不拦截，继续执行原 SQL
        return null;
    }

    @Override
    public boolean executeTopLevelOnly() {
        // 必须为 false，因为 Debezium 可能会在内部会话中执行此查询
        return false;
    }

    @Override
    public void destroy() {
    }

    @Override
    public <T extends Resultset> T postProcess(Supplier<String> sql, Query interceptedQuery, T originalResultSet,
                                               ServerSession serverSession) {
        return null;
    }
}