package whu.edu.cn.trajlab.example.index.time;

import junit.framework.TestCase;
import org.junit.Test;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.coding.coding.XZTCoding;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.db.index.time.IDTIndexStrategy;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;
import static whu.edu.cn.trajlab.example.index.spatial.XZ2IndexStrategyTest.getExampleTrajectory;

/**
 * @author xuqi
 * @date 2023/12/07
 */
public class IDTIndexStrategyTest extends TestCase {

  public void testIndex() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    XZTCoding XZTCoding = new XZTCoding();
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(XZTCoding);
    ByteArray index = IDTIndexStrategy.index(exampleTrajectory);
    System.out.println("ByteArray: " + index);
  }

  public void testGetTimeRange() {
    Trajectory exampleTrajectory = getExampleTrajectory();
    XZTCoding XZTCoding = new XZTCoding();
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(XZTCoding);
    ByteArray index = IDTIndexStrategy.index(exampleTrajectory);
    TimeLine timeLineRange = IDTIndexStrategy.getTimeLineRange(index);
    System.out.println(
        "timeStart: "
            + exampleTrajectory.getTrajectoryFeatures().getStartTime()
            + "timeEnd: "
            + exampleTrajectory.getTrajectoryFeatures().getEndTime());
    System.out.println(timeLineRange);
  }

  public void testGetSingleScanRanges() {
    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    ZonedDateTime start = ZonedDateTime.parse("2015-12-26 12:00:00", dateTimeFormatter);
    ZonedDateTime end = ZonedDateTime.parse("2015-12-26 13:00:00", dateTimeFormatter);
    TimeLine timeLine = new TimeLine(start, end);
    String Oid = "001";
    TemporalQueryCondition temporalQueryCondition =
        new TemporalQueryCondition(timeLine, TemporalQueryType.INTERSECT);
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
    List<RowKeyRange> scanRanges = IDTIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
    System.out.println("Single ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey())
              + " end : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey())
              + " isContained "
              + scanRange.isValidate());
    }
  }

  public void testGetMultiScanRange() {
    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    ZonedDateTime start1 = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end1 = ZonedDateTime.parse("2022-01-02 12:00:00", dateTimeFormatter);
    TimeLine timeLine1 = new TimeLine(start1, end1);
    ZonedDateTime start2 = ZonedDateTime.parse("2022-01-02 14:00:00", dateTimeFormatter);
    ZonedDateTime end2 = ZonedDateTime.parse("2022-01-03 16:00:00", dateTimeFormatter);
    TimeLine timeLine2 = new TimeLine(start2, end2);
    ArrayList<TimeLine> timeLines = new ArrayList<>();
    timeLines.add(timeLine1);
    timeLines.add(timeLine2);
    String Oid = "001";
    TemporalQueryCondition temporalQueryCondition =
        new TemporalQueryCondition(timeLines, TemporalQueryType.INTERSECT);
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
    List<RowKeyRange> scanRanges = IDTIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
    System.out.println("Multi ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey())
              + " end : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey())
              + " isContained "
              + scanRange.isValidate());
    }
  }

  public void testMultiInnerBinScan() {
    long start = System.currentTimeMillis();
    DateTimeFormatter dateTimeFormatter =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    ZonedDateTime start1 = ZonedDateTime.parse("2022-01-01 10:00:00", dateTimeFormatter);
    ZonedDateTime end1 = ZonedDateTime.parse("2022-01-01 12:00:00", dateTimeFormatter);
    TimeLine timeLine1 = new TimeLine(start1, end1);
    ZonedDateTime start2 = ZonedDateTime.parse("2022-01-01 14:00:00", dateTimeFormatter);
    ZonedDateTime end2 = ZonedDateTime.parse("2022-01-01 16:00:00", dateTimeFormatter);
    TimeLine timeLine2 = new TimeLine(start2, end2);
    ArrayList<TimeLine> timeLines = new ArrayList<>();
    timeLines.add(timeLine1);
    timeLines.add(timeLine2);
    String Oid = "001";
    TemporalQueryCondition temporalQueryCondition =
        new TemporalQueryCondition(timeLines, TemporalQueryType.INTERSECT);
    IDTIndexStrategy IDTIndexStrategy = new IDTIndexStrategy(new XZTCoding());
    List<RowKeyRange> scanRanges = IDTIndexStrategy.getScanRanges(temporalQueryCondition, Oid);
    System.out.println("Multi InnerBin ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey())
              + " end : "
              + IDTIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey())
              + " isContained "
              + scanRange.isValidate());
    }
    long end = System.currentTimeMillis();
    System.out.println(end - start);
  }
}
