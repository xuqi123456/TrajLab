package whu.edu.cn.trajlab.example.index.st;

import junit.framework.TestCase;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.condition.SpatialTemporalQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.db.index.spatial.XZ2IndexStrategy;
import whu.edu.cn.trajlab.db.index.spatialtemporal.TXZ2IndexStrategy;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class STIndexStrategyTest extends TestCase {
  public void testGetScanRanges() {
    WKTReader reader = new WKTReader();
    try {
      Geometry geom =
          reader.read(
              "POLYGON ((114.345703125 30.531005859375, 114.345703125 30.5419921875, 114.36767578125 30.5419921875, 114.36767578125 30.531005859375, 114.345703125 30.531005859375))");
      SpatialQueryCondition spatialQueryCondition =
          new SpatialQueryCondition(geom, SpatialQueryCondition.SpatialQueryType.INTERSECT);
      DateTimeFormatter dateTimeFormatter =
          DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
      ZonedDateTime start = ZonedDateTime.parse("2015-12-26 12:00:00", dateTimeFormatter);
      ZonedDateTime end = ZonedDateTime.parse("2015-12-26 13:00:00", dateTimeFormatter);
      TimeLine timeLine = new TimeLine(start, end);
      TemporalQueryCondition temporalQueryCondition =
          new TemporalQueryCondition(timeLine, TemporalQueryType.INTERSECT);
      TXZ2IndexStrategy txz2IndexStrategy = new TXZ2IndexStrategy();
      SpatialTemporalQueryCondition spatialTemporalQueryCondition =
          new SpatialTemporalQueryCondition(spatialQueryCondition, temporalQueryCondition);

      List<RowKeyRange> list = txz2IndexStrategy.getPartitionScanRanges(spatialTemporalQueryCondition);
      for (RowKeyRange range : list) {
        System.out.println(
            "start : "
                + txz2IndexStrategy.parseScanIndex2String(range.getStartKey())
                + " end : "
                + txz2IndexStrategy.parseScanIndex2String(range.getEndKey())
                + " isContained "
                + range.isValidate());
      }
    } catch (ParseException e) {
      e.printStackTrace();
    }
  }
}
