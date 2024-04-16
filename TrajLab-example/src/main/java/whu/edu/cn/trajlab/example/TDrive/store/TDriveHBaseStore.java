package whu.edu.cn.trajlab.example.TDrive.store;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.commons.io.FileSystemUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.operator.store.IStore;
import whu.edu.cn.trajlab.core.util.FSUtils;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.io.IOException;
import java.io.InputStream;

public class TDriveHBaseStore {
    private static final Logger LOGGER = LoggerFactory.getLogger(TDriveHBaseStore.class);

  public static void main(String[] args) throws IOException {
        /**
         * 1.读取配置文件
         * 提供三种方式：
         * HDFS，需要指定fs和路径（例：传入2参数localhost:9000 path）
         * 本地文件系统，需要指定路径（例：传入1参数 path）
         * 资源文件，不需传参，会从项目资源文件中读取，仅本地测试使用
         */
        String fileStr;
        if (args.length > 1) {
            String fs = args[0];
            String filePath = args[1];
            fileStr = FSUtils.readFromFS(fs, filePath);
        } else if (args.length == 1) {
            String confPath = args[0];
            fileStr = IOUtils.readFileToString(confPath);
        } else {
            InputStream resourceAsStream = TDriveHBaseStore.class.getClassLoader()
                    .getResourceAsStream("ioconf/tdrive/taxi_100_ID.json");
            fileStr = IOUtils.readFileToString(resourceAsStream);
        }
        // 本地测试时可以传入第三个参数，指定是否本地master运行
        boolean isLocal = false;
        int localIndex = 2;
        try {
            isLocal = Boolean.parseBoolean(args[localIndex]);
        } catch (Exception ignored) {
        }
        // 2.解析配置文件
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        // 3.初始化sparkSession
        LOGGER.info("Init sparkSession...");
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                TDriveHBaseStore.class.getName(), isLocal)) {
            // 4.加载轨迹数据
            ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
            JavaRDD<Trajectory> trajRDD =
                    iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig(),
                            exampleConfig.getDataConfig());
            long start = System.currentTimeMillis();
            // 5.存储轨迹数据
            IStore iStore = IStore.getStore(exampleConfig.getStoreConfig());
            iStore.storeTrajectory(trajRDD);
            long end = System.currentTimeMillis();
            long cost = (end - start);
            System.out.printf("HBase store cost %dmin %ds \n",cost /60000, cost%60000/1000);
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testDeleteDataSet() throws IOException {
        Database instance = Database.getInstance();
        instance.deleteDataSet("TDrive_100");
    }
}
