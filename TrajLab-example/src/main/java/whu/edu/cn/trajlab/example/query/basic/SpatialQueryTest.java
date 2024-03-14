package whu.edu.cn.trajlab.example.query.basic;

import com.fasterxml.jackson.core.JsonParseException;
import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.basic.SpatialQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class SpatialQueryTest extends TestCase {
  static String DATASET_NAME = "TRAJECTORY_TEST";
  public static SpatialQueryCondition spatialIntersectQueryCondition;
  public static SpatialQueryCondition spatialContainedQueryCondition;
  public static SpatialQueryCondition spatialContainedMinQueryCondition;
  static String QUERY_WKT_INTERSECT = "POLYGON ((116.32962772419637 39.972026172051926, 116.30270755784738 39.95837159882646, 116.2979676693152 39.941984580074575, 116.3236919401059 39.92376956848565, 116.35456902546804 39.92133821985189, 116.39416282850755 39.92224369651018, 116.4028739476417 39.94015883515999, 116.40010269765128 39.96383557395737, 116.37198885623383 39.972937650600954, 116.32962772419637 39.972026172051926))";
  static String QUERY_WKT_CONTAIN = "POLYGON ((116.13739069245264 40.15458565769407, 116.13739069245264 39.81560110525632, 116.54329729384301 39.81560110525632, 116.54329729384301 40.15458565769407, 116.13739069245264 40.15458565769407))";
  static String QUERY_WKT_CONTAINMin = "POLYGON ((116.29147294505526 39.97376857162786, 116.29147294505526 39.88293578040475, 116.38928044070065 39.88293578040475, 116.38928044070065 39.97376857162786, 116.29147294505526 39.97376857162786))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
      Geometry envelopeContained = wktReader.read(QUERY_WKT_CONTAIN);
      Geometry envelopeContainedMin = wktReader.read(QUERY_WKT_CONTAINMin);
      spatialIntersectQueryCondition =
          new SpatialQueryCondition(
              envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition =
          new SpatialQueryCondition(
              envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
      spatialContainedMinQueryCondition =
              new SpatialQueryCondition(
                      envelopeContainedMin, SpatialQueryCondition.SpatialQueryType.CONTAIN);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public void testExecuteRDDIntersectQuery() throws IOException {
    long start = System.currentTimeMillis();
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery =
        new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    boolean isLocal = true;
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
      JavaRDD<Trajectory> rddQuery = spatialQuery.getRDDQuery(sparkSession);
      List<Trajectory> results = rddQuery.collect();
      System.out.println(results.size());
      long end = System.currentTimeMillis();
      System.out.println("cost : " + (end - start) + "ms");
//      for (Trajectory result : results) {
//        ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
//        System.out.println(
//            indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
//        ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
//        ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
//        System.out.println(new TimeLine(startTime, endTime));
//      }
      int answer = getIntersectQueryAnswer(QUERY_WKT_INTERSECT, false);
      assertEquals(
              answer, results.size());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testExecuteIntersectQuery() throws IOException, ParseException {
    long start = System.currentTimeMillis();
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery =
        new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
//    boolean isLocal = true;
//    try (SparkSession sparkSession =
//                 SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
//      JavaSparkContext context = SparkUtils.getJavaSparkContext(sparkSession);
//      JavaRDD<Trajectory> parallelize = context.parallelize(results);
//      List<Trajectory> collect = parallelize.collect();
//    }
    long end = System.currentTimeMillis();
    System.out.println("cost : " + (end - start) + "ms");
//    for (Trajectory result : results) {
//      ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
//      System.out.println(
//          indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
//      ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
//      ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
//      System.out.println(new TimeLine(startTime, endTime));
//    }
    int answer = getIntersectQueryAnswer(QUERY_WKT_INTERSECT, false);
    assertEquals(
            answer, results.size());
  }

  public int getIntersectQueryAnswer(String queryWKT, boolean contain)
      throws ParseException, JsonParseException {
    List<Trajectory> trips = getLoadHBase();
    WKTReader wktReader = new WKTReader();
    Geometry query = wktReader.read(queryWKT);
    int res = 0;
    for (Trajectory trajectory : trips) {
      if (contain && query.contains(trajectory.getLineString())) {
        res++;
      } else if (!contain && query.intersects(trajectory.getLineString())) {
        res++;
      }
    }
    return res;
  }

  public void testExecuteContainedQuery() throws IOException, ParseException {
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery =
        new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialContainedQueryCondition);
    List<Trajectory> results = spatialQuery.executeQuery();
    System.out.println(results.size());
    for (Trajectory result : results) {
      ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
      System.out.println(
          indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
      ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
      ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
      System.out.println(new TimeLine(startTime, endTime));
    }
    int answer = getIntersectQueryAnswer(QUERY_WKT_CONTAIN, true);
    assertEquals(answer, results.size());
  }

  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }
}
