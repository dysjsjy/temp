package com.dysjsjy.TableRandData.demo2;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.javafaker.Faker;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Test {
    private static final Logger logger = LoggerFactory.getLogger(Test.class);
    private Faker faker = new Faker(new Locale("zh-CN"));

    private Map<String, List<Object>> primaryKeyCache = new HashMap<>();

    private HikariDataSource dataSource;

    public static void main(String[] args) throws Exception {
        Test test = null;
        try {
            test = new Test();


//        List<String> tag = test.getTableColumns(test.getConnection(), "tag");
//        System.out.println(tag);
//            test.clearTable("tag");
            test.fillTablesFromConfig("src/main/resources/test.json");

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            test.close();
        }

    }

    public Test() {

        // String url = "jdbc:mysql://localhost:3306/dyumb?useSSL=true&characterEncoding=utf8";
        // String user = "root";
        // String password = "root";
        // return DriverManager.getConnection(url, user, password);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:mysql://localhost:3306/dyumb?useSSL=true");
        config.setUsername("root");
        config.setPassword("root");
        config.setMaximumPoolSize(10); // 连接池大小
        dataSource = new HikariDataSource(config);
    }

    private Connection getConnection() throws Exception {
        return dataSource.getConnection();
    }

    // 表配置类
    static class TableConfig {
        @JsonProperty("tableName")
        String tableName;

        @JsonProperty("rowCount")
        int rowCount;

        @JsonProperty("fields")
        List<FieldConfig> fields;

        static class FieldConfig {
            @JsonProperty("name")
            String name;

            @JsonProperty("type")
            String type;

            @JsonProperty("rule")
            String rule;
        }
    }

    // 从JSON配置文件填充多张表
    public void fillTablesFromConfig(String configFile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TableConfig[] tables = mapper.readValue(new File(configFile), TableConfig[].class);

        for (TableConfig table : tables) {
            long startTime = System.currentTimeMillis();
            fillTable(table);
            logger.info("Filled table {} in {} ms", table.tableName, System.currentTimeMillis() - startTime);
        }
    }

    public void fillTablesFromConfigWithThreads(String configFile) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        TableConfig[] tables = mapper.readValue(new File(configFile), TableConfig[].class);
        ExecutorService executor = Executors.newFixedThreadPool(4); // 4个线程
        for (TableConfig table : tables) {
            executor.submit(() -> {
                try {
                    fillTable(table);
                } catch (Exception e) {
                    logger.error("Failed to fill table {}: {}", table.tableName, e.getMessage());
                }
            });
        }
        executor.shutdown();
        executor.awaitTermination(1, java.util.concurrent.TimeUnit.HOURS);
    }

    // 填充单张表
    private void fillTable(TableConfig table) throws Exception {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);  // 移到最前面

            try {
                // 动态生成INSERT语句
                StringJoiner columns = new StringJoiner(", ");
                StringJoiner placeholders = new StringJoiner(", ");
                for (TableConfig.FieldConfig field : table.fields) {
                    columns.add(field.name);
                    placeholders.add("?");
                }
                String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                        table.tableName, columns, placeholders);

                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    // 批量插入数据
                    for (int i = 0; i < table.rowCount; i++) {
                        int paramIndex = 1;
                        for (TableConfig.FieldConfig field : table.fields) {
                            setParameter(pstmt, paramIndex++, field, i);
                        }
                        pstmt.addBatch();

                        if (i % 1000 == 0) {
                            pstmt.executeBatch();
                            conn.commit();
                        }
                    }

                    pstmt.executeBatch();

                    logger.info("Inserted {} rows into {}", table.rowCount, table.tableName);
                }

            } catch (Exception e) {
                conn.rollback();
                logger.error("Transaction rolled back for table {} due to error: {}",
                    table.tableName, e.getMessage(), e);  // 添加更详细的错误日志
                throw new SQLException("Failed to insert into " + table.tableName + ": " + e.getMessage(), e);
            } finally {
                conn.setAutoCommit(true);  // 确保恢复自动提交
            }
        }
    }

    // 根据字段规则设置参数
    private void setParameter(PreparedStatement pstmt, int index, TableConfig.FieldConfig field, int rowIndex) throws Exception {
        String rule = field.rule;
        switch (field.type.toLowerCase()) {
            case "varchar":
                if (rule.startsWith("faker.lorem.word")) {
                    pstmt.setString(index, faker.lorem().word() + (rowIndex + 1));
                } else if (rule.startsWith("faker.name")) {
                    pstmt.setString(index, faker.name().fullName());
                } else {
                    pstmt.setString(index, rule);
                }
                break;
            case "bigint":
                if (rule.startsWith("random(")) {
                    String[] range = rule.substring(rule.indexOf("(") + 1, rule.indexOf(")")).split(",");
                    long min = Long.parseLong(range[0].trim());  // 添加trim()
                    long max = Long.parseLong(range[1].trim());  // 添加trim()
                    pstmt.setLong(index, faker.number().numberBetween(min, max));
                } else if (rule.equals("increment")) {
                    pstmt.setLong(index, rowIndex + 1);
                } else if (rule.startsWith("random_boolean ?")) {
                    String[] parts = rule.split(" : ");
                    long value = faker.random().nextBoolean() ? Long.parseLong(parts[0].split(" \\? ")[1].trim()) :  // 添加trim()
                            Long.parseLong(parts[1].substring(parts[1].indexOf("(") + 1, parts[1].indexOf(")")).split(",")[1].trim());  // 添加trim()
                    pstmt.setLong(index, value);
                }
                break;
            case "int":
                if (rule.startsWith("random(")) {
                    String[] range = rule.substring(rule.indexOf("(") + 1, rule.indexOf(")")).split(",");
                    int min = Integer.parseInt(range[0].trim());
                    int max = Integer.parseInt(range[1].trim());
                    pstmt.setInt(index, faker.number().numberBetween(min, max));
                } else if (rule.startsWith("parentId == 0")) {
                    boolean isParent = rule.contains("? 1 : 0");
                    pstmt.setInt(index, isParent ? 1 : 0);
                } else if (rule.startsWith("fixed(")) {
                    pstmt.setInt(index, Integer.parseInt(rule.substring(rule.indexOf("(") + 1, rule.indexOf(")"))));
                }
                break;
            case "decimal":
                if (rule.startsWith("random(")) {
                    String[] range = rule.substring(rule.indexOf("(") + 1, rule.indexOf(")")).split(",");
                    double min = Double.parseDouble(range[0].trim());
                    double max = Double.parseDouble(range[1].trim());
                    pstmt.setDouble(index, min + (max - min) * faker.random().nextDouble());
                }
                break;
            case "timestamp":
                if (rule.equals("now")) {
                    pstmt.setTimestamp(index, new Timestamp(System.currentTimeMillis()));
                }
                break;
            default:
                throw new IllegalArgumentException("Unsupported field type: " + field.type);
        }
    }


    private void cachePrimaryKeys(Connection conn, String table, String primaryKeyColumn) throws Exception {
        List<Object> keys = new ArrayList<>();
        String sql = "SELECT " + primaryKeyColumn + " FROM " + table;
        try (PreparedStatement pstmt = conn.prepareStatement(sql); var rs = pstmt.executeQuery()) {
            while (rs.next()) {
                keys.add(rs.getObject(1));
            }
        }
        primaryKeyCache.put(table + "." + primaryKeyColumn, keys);
    }

    private List<String> getTableColumns(Connection conn, String tableName) throws Exception {
        List<String> columns = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (var rs = meta.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    private int verifyRowCount(Connection conn, String tableName) throws Exception {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
             var rs = pstmt.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    public void clearTable(String tableName) throws Exception {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName)) {
                pstmt.executeUpdate();
                conn.commit();
                logger.info("Cleared table {}", tableName);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }
}