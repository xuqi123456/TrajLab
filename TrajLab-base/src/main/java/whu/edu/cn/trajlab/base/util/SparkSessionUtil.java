package whu.edu.cn.trajlab.base.util;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class SparkSessionUtil {
    private static final Logger logger = Logger.getLogger(SparkSessionUtil.class);

    public static SparkSession createSession(String className, boolean isLocal) {
        SparkConf sparkConf = new SparkConf();
        sparkConf.set("fs.permissions.umask-mode", "022");
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        sparkConf.set("spark.kryoserializer.buffer.max", "256m");
        sparkConf.set("spark.kryoserializer.buffer", "64m");
        if (isLocal) {
            sparkConf.setMaster("local[*]");
        }

        return SparkSession.builder()
                .appName(className + "_" + System.currentTimeMillis())
                .config(sparkConf)
                .getOrCreate();
    }
}
