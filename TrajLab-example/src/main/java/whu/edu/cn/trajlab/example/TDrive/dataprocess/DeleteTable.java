package whu.edu.cn.trajlab.example.TDrive.dataprocess;

import whu.edu.cn.trajlab.db.database.Database;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2024/04/10
 */
public class DeleteTable {
    public static void main(String[] args) throws IOException {
        String name = args[0];
        Database instance = Database.getInstance();
        instance.deleteDataSet(name);
    }
}
