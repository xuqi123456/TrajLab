package whu.edu.cn.trajlab.base.util;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * @author xuqi
 * @date 2024/01/22
 */
public class SparkUtils {
    public static JavaSparkContext getJavaSparkContext(SparkSession sc) {
        return new JavaSparkContext(sc.sparkContext());
    }
}
