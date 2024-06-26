package whu.edu.cn.trajlab.example.util;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.core.conf.load.ILoadConfig;

/**
 * @author xuqi
 * @date 2023/11/16
 */
public class SparkSessionUtils {
  private static final Logger logger = Logger.getLogger(SparkSessionUtils.class);

  /**
   * 创建sparkSession
   *
   * @param loadConfig 输入配置
   * @param className 进入的类名称
   * @return : org.apache.spark.sql.SparkSession SparkSession
   */
  public static SparkSession createSession(
          ILoadConfig loadConfig, String className, boolean isLocal) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("fs.permissions.umask-mode", "022");
    if (loadConfig.getFsDefaultName() != null) {
      sparkConf.set("fs.defaultFS", loadConfig.getFsDefaultName());
    }
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryoserializer.buffer.max", "1024m");
    sparkConf.set("spark.kryoserializer.buffer", "256m");
    sparkConf.set("spark.default.parallelism", "24");
    if (isLocal) {
      sparkConf.setMaster("local[*]");
    }
    switch (loadConfig.getInputType()) {
      case STANDALONE:
      case HDFS:
      case HBASE:
      case GEOMESA:
        return SparkSession.builder()
            .appName(className + "_" + System.currentTimeMillis())
            .config(sparkConf)
            .getOrCreate();
      default:
        logger.error("Only HDFS and HIVE are supported as the input resource!");
        throw new NoSuchMethodError();
    }
  }

  public static SparkSession createSession(String className, boolean isLocal) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("fs.permissions.umask-mode", "022");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryoserializer.buffer.max", "1024m");
    sparkConf.set("spark.kryoserializer.buffer", "256m");
    sparkConf.set("spark.default.parallelism", "24");
    if (isLocal) {
      sparkConf.setMaster("local[*]");
    }

    return SparkSession.builder()
        .appName(className + "_" + System.currentTimeMillis())
        .config(sparkConf)
        .getOrCreate();
  }

  public static SparkSession createSession(String className, boolean isLocal, String partition) {
    SparkConf sparkConf = new SparkConf();
    sparkConf.set("fs.permissions.umask-mode", "022");
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
    sparkConf.set("spark.kryoserializer.buffer.max", "1024m");
    sparkConf.set("spark.kryoserializer.buffer", "256m");
    sparkConf.set("spark.default.parallelism", partition);
    if (isLocal) {
      sparkConf.setMaster("local[*]");
    }

    return SparkSession.builder()
            .appName(className + "_" + System.currentTimeMillis())
            .config(sparkConf)
            .getOrCreate();
  }
}
