package whu.edu.cn.trajlab.example.TDrive.basicQuery.spatial;

import static whu.edu.cn.trajlab.query.query.QueryConf.*;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.example.TDrive.basicQuery.CreateQueryWindow;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.BasicQuery;

/**
 * @author xuqi
 * @date 2024/04/11
 */
public class RDDTDriveSpatialQuery {
  public static void main(String[] args) throws IOException, ParseException {
    String dataSetName = args[0];
    String querySize = args[1];
    String queryWindow = CreateQueryWindow.createSpatialQueryWindow(Double.parseDouble(querySize));

    long start;
    long end;
    List<Trajectory> collect;
    Configuration conf = new Configuration();
    conf.set(INDEX_TYPE, "XZ2");
    conf.set(DATASET_NAME, dataSetName);
    conf.set(SPATIAL_WINDOW, queryWindow);
    BasicQuery basicQuery1 = new BasicQuery(conf);
    long startAll = System.currentTimeMillis();
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(HBaseDataStore.class.getName(), true)) {
      start = System.currentTimeMillis();
      JavaRDD<Trajectory> rddScanQuery = basicQuery1.getRDDScanQuery(sparkSession);
      collect = rddScanQuery.collect();
      end = System.currentTimeMillis();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    long endAll = System.currentTimeMillis();
    long cost = (end - start);
    long costAll = (endAll - startAll);

    // 计算分钟和剩余秒数
    long minutes = cost / 60000;
    long seconds = (cost % 60000) / 1000;

    // 计算分钟和剩余秒数
    long minutesAll = costAll / 60000;
    long secondsAll = (costAll % 60000) / 1000;
    System.out.println("Distributed Data Size : " + collect.size());
    // 使用 printf 格式化字符串打印时间
    System.out.printf("Distributed Query cost %d minutes %d seconds\n", minutes, seconds);
    System.out.printf("ToTal Distributed Query cost %d minutes %d seconds\n", minutesAll, secondsAll);
  }
}
