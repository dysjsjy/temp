package com.dysjsjy.TableRandData.demo1;


import com.github.javafaker.Faker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.Locale;

public class Test1 {

    private static final Logger logger = LoggerFactory.getLogger(Test1.class);

    public static void main(String[] args) throws Exception {
//        logger.info("fucking hello world");

        Test1 test = new Test1();
        test.fillTable("tag", 1000);
    }

    private Connection getConnection() throws Exception {
        String url = "jdbc:mysql://localhost:3306/dyumb?useSSL=true";
        String user = "root";
        String password = "root";
        return DriverManager.getConnection(url, user, password);
    }

    // 这里提供了一个MVP，但是在有上百张表的情况下再手动写固定的insert语句是不是有点重复工作了

    public void fillTable(String tableName, int rowCount) throws Exception {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            String sql = "INSERT INTO " + tableName + " (tagName, userId, parentId, isParent, createTime, updateTime, isDelete) VALUES (?, ?, ?, ?, ?, ?, ?)";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            Faker faker = new Faker(new Locale("zh-CN"));

            for (int i = 0; i < rowCount; i++) {
                pstmt.setString(1, faker.lorem().word() + (i + 1));  // tagName
                pstmt.setLong(2, faker.number().numberBetween(1L, 10000L));  // userId
                long parentId = faker.random().nextBoolean() ? 0L : faker.number().numberBetween(1L, 100L);
                pstmt.setLong(3, parentId);  // parentId
                pstmt.setInt(4, parentId == 0L ? 1 : 0);  // isParent
                Timestamp now = new Timestamp(System.currentTimeMillis());
                pstmt.setTimestamp(5, now);  // createTime
                pstmt.setTimestamp(6, now);  // updateTime
                pstmt.setInt(7, 0);  // isDelete
                pstmt.addBatch();

                if (i % 1000 == 0) {
                    pstmt.executeBatch();
                    conn.commit();
                }
            }

            pstmt.executeBatch();
            conn.commit();
            System.out.println("Inserted " + rowCount + " rows into " + tableName);
        }
    }
}
