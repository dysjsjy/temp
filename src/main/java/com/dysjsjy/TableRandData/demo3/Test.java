package com.dysjsjy.TableRandData.demo3;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;

public class Test {
    private static final Logger logger = Logger.getLogger(Test.class.getName());
    private Random random = new Random();
    private Map<String, List<Object>> primaryKeyCache = new HashMap<>();

    public static void main(String[] args) throws Exception {
        Test test = null;
        try {
            test = new Test();
//            test.fillTablesFromConfig("src/main/resources/test2.json");
            test.clearTable("tag");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (test != null) {
                test.close();
            }
        }
    }

    public Test() {
        // No connection pooling; connections are managed manually
    }

    private Connection getConnection() throws SQLException {
        String url = "jdbc:mysql://localhost:3306/dyumb?useSSL=true&characterEncoding=utf8";
        String user = "root";
        String password = "root";
        return DriverManager.getConnection(url, user, password);
    }

    // Table configuration class
    static class TableConfig {
        String tableName;
        int rowCount;
        List<FieldConfig> fields;

        static class FieldConfig {
            String name;
            String type;
            String rule;
        }
    }

    // Parse JSON configuration manually (simple parser for arrays of objects)
    public void fillTablesFromConfig(String configFile) throws Exception {
        List<TableConfig> tables = parseJsonConfig(configFile);
        for (TableConfig table : tables) {
            long startTime = System.currentTimeMillis();
            fillTable(table);
            logger.info("Filled table " + table.tableName + " in " + (System.currentTimeMillis() - startTime) + " ms");
        }
    }

    // Simple manual JSON parser for the expected structure
    private List<TableConfig> parseJsonConfig(String configFile) throws IOException {
        List<TableConfig> tables = new ArrayList<>();
        StringBuilder json = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(configFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                json.append(line.trim());
            }
        }

        // Remove outer brackets
        String content = json.toString().trim();
        if (content.startsWith("[") && content.endsWith("]")) {
            content = content.substring(1, content.length() - 1);
        }

        // Split into table entries
        String[] tableEntries = content.split("\\},\\{");
        for (String entry : tableEntries) {
            entry = entry.replaceAll("[{}\"]", "").trim();
            if (entry.isEmpty()) continue;

            TableConfig table = new TableConfig();
            String[] fields = entry.split(",");
            table.fields = new ArrayList<>();

            for (String field : fields) {
                String[] keyValue = field.split(":");
                if (keyValue.length != 2) continue;
                String key = keyValue[0].trim();
                String value = keyValue[1].trim();

                if (key.equals("tableName")) {
                    table.tableName = value;
                } else if (key.equals("rowCount")) {
                    table.rowCount = Integer.parseInt(value);
                } else if (key.equals("fields")) {
                    // Handle fields array
                    String fieldsContent = value.substring(value.indexOf("[") + 1, value.lastIndexOf("]"));
                    String[] fieldEntries = fieldsContent.split("\\},\\{");
                    for (String fieldEntry : fieldEntries) {
                        fieldEntry = fieldEntry.replaceAll("[{}\"]", "").trim();
                        String[] fieldProps = fieldEntry.split(",");
                        TableConfig.FieldConfig fieldConfig = new TableConfig.FieldConfig();
                        for (String prop : fieldProps) {
                            String[] propKeyValue = prop.split(":");
                            if (propKeyValue.length != 2) continue;
                            String propKey = propKeyValue[0].trim();
                            String propValue = propKeyValue[1].trim();
                            if (propKey.equals("name")) {
                                fieldConfig.name = propValue;
                            } else if (propKey.equals("type")) {
                                fieldConfig.type = propValue;
                            } else if (propKey.equals("rule")) {
                                fieldConfig.rule = propValue;
                            }
                        }
                        table.fields.add(fieldConfig);
                    }
                }
            }
            tables.add(table);
        }
        return tables;
    }

    // Fill a single table
    private void fillTable(TableConfig table) throws Exception {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);

            try {
                StringJoiner columns = new StringJoiner(", ");
                StringJoiner placeholders = new StringJoiner(", ");
                for (TableConfig.FieldConfig field : table.fields) {
                    columns.add(field.name);
                    placeholders.add("?");
                }
                String sql = String.format("INSERT INTO %s (%s) VALUES (%s)",
                        table.tableName, columns, placeholders);

                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
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
                    conn.commit();
                    logger.info("Inserted " + table.rowCount + " rows into " + table.tableName);
                }
            } catch (Exception e) {
                conn.rollback();
                logger.severe("Transaction rolled back for table " + table.tableName + " due to error: " + e.getMessage());
                throw new SQLException("Failed to insert into " + table.tableName + ": " + e.getMessage(), e);
            } finally {
                conn.setAutoCommit(true);
            }
        }
    }

    // Set parameter based on field rules
    private void setParameter(PreparedStatement pstmt, int index, TableConfig.FieldConfig field, int rowIndex) throws Exception {
        String rule = field.rule;
        switch (field.type.toLowerCase()) {
            case "varchar":
                if (rule.startsWith("faker.lorem.word")) {
                    pstmt.setString(index, generateRandomWord() + (rowIndex + 1));
                } else if (rule.startsWith("faker.name")) {
                    pstmt.setString(index, generateRandomName());
                } else {
                    pstmt.setString(index, rule);
                }
                break;
            case "bigint":
                if (rule.startsWith("random(")) {
                    String[] range = rule.substring(rule.indexOf("(") + 1, rule.indexOf(")")).split(",");
                    long min = Long.parseLong(range[0].trim());
                    long max = Long.parseLong(range[1].trim());
                    pstmt.setLong(index, min + (long) (random.nextDouble() * (max - min)));
                } else if (rule.equals("increment")) {
                    pstmt.setLong(index, rowIndex + 1);
                } else if (rule.startsWith("random_boolean ?")) {
                    String[] parts = rule.split(" : ");
                    long value = random.nextBoolean() ? Long.parseLong(parts[0].split(" \\? ")[1].trim()) :
                            Long.parseLong(parts[1].substring(parts[1].indexOf("(") + 1, parts[1].indexOf(")")).split(",")[1].trim());
                    pstmt.setLong(index, value);
                }
                break;
            case "int":
                if (rule.startsWith("random(")) {
                    String[] range = rule.substring(rule.indexOf("(") + 1, rule.indexOf(")")).split(",");
                    int min = Integer.parseInt(range[0].trim());
                    int max = Integer.parseInt(range[1].trim());
                    pstmt.setInt(index, min + random.nextInt(max - min));
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
                    pstmt.setDouble(index, min + (max - min) * random.nextDouble());
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

    // Simple random word generator
    private String generateRandomWord() {
        String[] words = {"apple", "book", "car", "dog", "elephant", "forest", "garden", "house", "island", "jungle"};
        return words[random.nextInt(words.length)];
    }

    // Simple random name generator
    private String generateRandomName() {
        String[] firstNames = {"Li", "Wang", "Zhang", "Liu", "Chen"};
        String[] lastNames = {"Wei", "Jie", "Hao", "Ming", "Xin"};
        return firstNames[random.nextInt(firstNames.length)] + " " + lastNames[random.nextInt(lastNames.length)];
    }

    private void cachePrimaryKeys(Connection conn, String table, String primaryKeyColumn) throws SQLException {
        List<Object> keys = new ArrayList<>();
        String sql = "SELECT " + primaryKeyColumn + " FROM " + table;
        try (PreparedStatement pstmt = conn.prepareStatement(sql); ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                keys.add(rs.getObject(1));
            }
        }
        primaryKeyCache.put(table + "." + primaryKeyColumn, keys);
    }

    private List<String> getTableColumns(Connection conn, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        DatabaseMetaData meta = conn.getMetaData();
        try (ResultSet rs = meta.getColumns(null, null, tableName, null)) {
            while (rs.next()) {
                columns.add(rs.getString("COLUMN_NAME"));
            }
        }
        return columns;
    }

    private int verifyRowCount(Connection conn, String tableName) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
             ResultSet rs = pstmt.executeQuery()) {
            rs.next();
            return rs.getInt(1);
        }
    }

    public void clearTable(String tableName) throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement pstmt = conn.prepareStatement("DELETE FROM " + tableName)) {
                pstmt.executeUpdate();
                conn.commit();
                logger.info("Cleared table " + tableName);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    public void close() {
        // No connection pool to close
    }
}