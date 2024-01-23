package whu.edu.cn.trajlab.example.query.basic;

import com.fasterxml.jackson.core.JsonParseException;
import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
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
  static String DATASET_NAME = "query_test";
  public static SpatialQueryCondition spatialIntersectQueryCondition;
  public static SpatialQueryCondition spatialContainedQueryCondition;
  static String QUERY_WKT_INTERSECT =
      "POLYGON((114.05185384869783 22.535191684309407,114.07313985944002 22.535191684309407,114.07313985944002 22.51624317521578,114.05185384869783 22.51624317521578,114.05185384869783 22.535191684309407))";
  static String QUERY_WKT_CONTAIN =
      "POLYGON((114.06266851544588 22.55279006251164,114.09511251569002 22.55263152858115,114.09631414532869 22.514023096146417,114.02833624005525 22.513705939082808,114.02799291730135 22.553107129826113,114.06266851544588 22.55279006251164))";

  static {
    System.setProperty("hadoop.home.dir", "/usr/local/hadoop-2.7.7");
    try {
      WKTReader wktReader = new WKTReader();
      Geometry envelopeIntersect = wktReader.read(QUERY_WKT_INTERSECT);
      Geometry envelopeContained = wktReader.read(QUERY_WKT_CONTAIN);
      spatialIntersectQueryCondition =
          new SpatialQueryCondition(
              envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      spatialContainedQueryCondition =
          new SpatialQueryCondition(
              envelopeContained, SpatialQueryCondition.SpatialQueryType.CONTAIN);
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public void testExecuteRDDIntersectQuery() throws IOException {
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
      for (Trajectory result : results) {
        ByteArray index = indexTable.getIndexMeta().getIndexStrategy().index(result);
        System.out.println(
            indexTable.getIndexMeta().getIndexStrategy().parsePhysicalIndex2String(index));
        ZonedDateTime startTime = result.getTrajectoryFeatures().getStartTime();
        ZonedDateTime endTime = result.getTrajectoryFeatures().getEndTime();
        System.out.println(new TimeLine(startTime, endTime));
      }
      assertEquals(
          getIntersectQueryAnswer(QUERY_WKT_INTERSECT, false), results.size());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void testExecuteIntersectQuery() throws IOException, ParseException {
    Database instance = Database.getInstance();
    IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
    SpatialQuery spatialQuery =
        new SpatialQuery(instance.getDataSet(DATASET_NAME), spatialIntersectQueryCondition);
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
    assertEquals(
        getIntersectQueryAnswer(QUERY_WKT_INTERSECT, false), results.size());
  }

  public int getIntersectQueryAnswer(String queryWKT, boolean contain)
      throws ParseException, JsonParseException {
    JavaRDD<Trajectory> loadHBase = getLoadHBase();
    List<Trajectory> trips = loadHBase.collect();
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

  public void testExecuteContainedQuery() throws IOException {
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
    assertEquals(19, results.size());
  }

  public void testDeleteDataSet() throws IOException {
    Database instance = Database.getInstance();
    instance.deleteDataSet(DATASET_NAME);
  }
}
