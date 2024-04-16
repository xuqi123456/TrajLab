package whu.edu.cn.trajlab.example.TDrive.basicQuery.ts;

import static whu.edu.cn.trajlab.query.query.QueryConf.*;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.locationtech.jts.io.ParseException;
import scala.Tuple2;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.enums.TimePeriod;
import whu.edu.cn.trajlab.example.TDrive.basicQuery.CreateQueryWindow;
import whu.edu.cn.trajlab.query.query.BasicQuery;

/**
 * @author xuqi
 * @date 2024/04/11
 */
public class TDriveTSQuery {
  public static void main(String[] args) throws IOException, ParseException {
    String dataSetName = args[0];
    String spatialSize = args[1];
    String querySize = args[2];
    String timePeriod = args[3];
    String spatialQueryWindow = CreateQueryWindow.createSpatialQueryWindow(Double.parseDouble(spatialSize));
    Tuple2<String, String> temproalQueryWindow = CreateQueryWindow.createTemporalQueryWindow(
            Double.parseDouble(querySize), TimePeriod.valueOf(timePeriod));
    long start = System.currentTimeMillis();
    Configuration conf = new Configuration();
    conf.set(INDEX_TYPE, "TXZ2");
    conf.set(DATASET_NAME, dataSetName);
    conf.set(SPATIAL_WINDOW, spatialQueryWindow);
    conf.set(START_TIME, temproalQueryWindow._1);
    conf.set(END_TIME, temproalQueryWindow._2);
    BasicQuery basicQuery = new BasicQuery(conf);
    List<Trajectory> scanQuery = basicQuery.getScanQuery();
    long end = System.currentTimeMillis();
    long cost = (end - start);
    System.out.println("Data Size : " + scanQuery.size());
    System.out.printf("Query cost %dmin %ds \n", cost / 60000, cost % 60000 / 1000);
  }
}
