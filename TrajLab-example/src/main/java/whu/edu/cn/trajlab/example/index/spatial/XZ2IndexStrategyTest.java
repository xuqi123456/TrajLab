package whu.edu.cn.trajlab.example.index.spatial;

import junit.framework.TestCase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.TrajFeatures;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.db.index.spatial.XZ2IndexStrategy;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;


/**
 * @author xuqi
 * @date 2023/12/07
 */
public class XZ2IndexStrategyTest extends TestCase {

  public static Trajectory getExampleTrajectory() {
    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    ZonedDateTime start = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2022-01-01 10:20:00", dateTimeFormatter);
    TrajFeatures trajFeatures =
        new TrajFeatures(
            start,
            end,
            new TrajPoint(start, 114.364672, 30.535034),
            new TrajPoint(end, 114.378505, 30.544039),
            2,
            null,
            0,
            2);
    return new Trajectory(
        "Trajectory demo",
        "001",
        Arrays.asList(
            new TrajPoint(start, 114.06896, 22.542664),
            new TrajPoint(start.plus(5L, ChronoUnit.MINUTES), 114.08942, 22.543316),
            new TrajPoint(start.plus(10L, ChronoUnit.MINUTES), 114.116684, 22.547997),
            new TrajPoint(start.plus(15L, ChronoUnit.MINUTES), 114.118904, 22.562414),
            new TrajPoint(start.plus(20L, ChronoUnit.MINUTES), 114.10953, 22.59049)),
        trajFeatures);
  }

  public void testIndex() {
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    System.out.println(XZ2IndexStrategy.index(t));
  }

  public void testGetScanRanges() {
    WKTReader reader = new WKTReader();
    try {
      Geometry geom =
          reader.read(
              "POLYGON ((114.345703125 30.531005859375, 114.345703125 30.5419921875, 114.36767578125 30.5419921875, 114.36767578125 30.531005859375, 114.345703125 30.531005859375))");
      SpatialQueryCondition spatialQueryCondition =
          new SpatialQueryCondition(geom, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
      List<RowKeyRange> list = XZ2IndexStrategy.getPartitionScanRanges(spatialQueryCondition);
      for (RowKeyRange range : list) {
        System.out.println(
                "start : "
                        + XZ2IndexStrategy.parseScanIndex2String(range.getStartKey())
                        + " end : "
                        + XZ2IndexStrategy.parseScanIndex2String(range.getEndKey())
                        + " isContained "
                        + range.isValidate());
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }

  public void testIndexToString() {
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    ByteArray byteArray = XZ2IndexStrategy.index(t);
    System.out.println(XZ2IndexStrategy.parsePhysicalIndex2String(byteArray));
  }

  public void testGetShardNum() {}

  public void testGetTrajectoryId() {
    Trajectory t = getExampleTrajectory();
    XZ2IndexStrategy XZ2IndexStrategy = new XZ2IndexStrategy();
    ByteArray byteArray = XZ2IndexStrategy.index(t);
    System.out.println(
        "Actual: "
            + XZ2IndexStrategy.getObjectID(byteArray)
            + "-"
            + XZ2IndexStrategy.getTrajectoryID(byteArray));
    System.out.println("Expected: " + t.getObjectID() + "-" + t.getTrajectoryID());
  }
}
