package com.dysjsjy.temp;

import java.io.RandomAccessFile;
import java.io.FileNotFoundException;
import java.io.IOException;

public class Temp {
    public static void main(String[] args) {
        try {
            // 创建RandomAccessFile对象，第二个参数"rw"表示可读可写
            RandomAccessFile randomAccessFile = new RandomAccessFile("test.txt", "rw");

            // 写入数据
            randomAccessFile.writeUTF("Hello World");

            // 移动到文件开头
            randomAccessFile.seek(0);

            // 读取数据
            System.out.println(randomAccessFile.readUTF());

            // 关闭文件
            randomAccessFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
