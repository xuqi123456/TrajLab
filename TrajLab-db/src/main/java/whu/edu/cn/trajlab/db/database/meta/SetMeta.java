package whu.edu.cn.trajlab.db.database.meta;

import scala.Tuple4;
import whu.edu.cn.trajlab.base.util.DateUtils;
import whu.edu.cn.trajlab.db.constant.SetConstants;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.base.mbr.MinimumBoundingBox;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author xuqi
 * @date 2023/12/05
 */
public class SetMeta implements Serializable {
  private static Logger logger = LoggerFactory.getLogger(SetMeta.class);
  private ZonedDateTime start_time = DateUtils.parseDate(SetConstants.start_time);
  private int srid = SetConstants.srid;
  private MinimumBoundingBox boundingBox;
  private TimeLine timeLine;
  private int dataCount;

  public SetMeta(JavaRDD<Trajectory> trajectoryJavaRDD) {
    this.buildSetMetaFromRDD(trajectoryJavaRDD);
  }

  public SetMeta(
      ZonedDateTime start_time,
      int srid,
      MinimumBoundingBox boundingBox,
      TimeLine timeLine,
      int dataCount) {
    this.start_time = start_time;
    this.srid = srid;
    this.boundingBox = boundingBox;
    this.timeLine = timeLine;
    this.dataCount = dataCount;
  }

  public SetMeta(MinimumBoundingBox boundingBox, TimeLine timeLine, int dataCount) {
    this.boundingBox = boundingBox;
    this.timeLine = timeLine;
    this.dataCount = dataCount;
  }

  public static Logger getLogger() {
    return logger;
  }

  public ZonedDateTime getStart_time() {
    return start_time;
  }

  public int getSrid() {
    return srid;
  }

  public MinimumBoundingBox getBoundingBox() {
    return boundingBox;
  }

  public TimeLine getTimeLine() {
    return timeLine;
  }

  public int getDataCount() {
    return dataCount;
  }

  public void buildSetMetaFromRDD(JavaRDD<Trajectory> trajectoryJavaRDD) {
    trajectoryJavaRDD.foreachPartition(
        t -> {
          if (t.hasNext()) {
            t.next().getTrajectoryFeatures();
          }
        });

    JavaRDD<Tuple4<MinimumBoundingBox, ZonedDateTime, ZonedDateTime, Integer>> result =
        trajectoryJavaRDD.mapPartitions(
            partitionItr -> {
              List<Tuple4<MinimumBoundingBox, ZonedDateTime, ZonedDateTime, Integer>> localBoxes =
                  new ArrayList<>();
              if (partitionItr.hasNext()) {
                MinimumBoundingBox localBox = null;
                ZonedDateTime localStartTime = null;
                ZonedDateTime localEndTime = null;
                Integer localCount = 0;
                while (partitionItr.hasNext()) {
                  Trajectory t = partitionItr.next();
                  if (localBox == null) {
                    localBox = t.getTrajectoryFeatures().getMbr();
                    localStartTime = t.getTrajectoryFeatures().getStartTime();
                    localEndTime = t.getTrajectoryFeatures().getEndTime();
                    localCount++;
                  } else {
                    localBox = localBox.union(t.getTrajectoryFeatures().getMbr());
                    localStartTime =
                        localStartTime.compareTo(t.getTrajectoryFeatures().getStartTime()) <= 0
                            ? localStartTime
                            : t.getTrajectoryFeatures().getStartTime();
                    localEndTime =
                        localEndTime.compareTo(t.getTrajectoryFeatures().getEndTime()) >= 0
                            ? localEndTime
                            : t.getTrajectoryFeatures().getEndTime();
                    localCount++;
                  }
                }
                localBoxes.add(new Tuple4<>(localBox, localStartTime, localEndTime, localCount));
              }
              return localBoxes.iterator();
            });
    Tuple4<MinimumBoundingBox, ZonedDateTime, ZonedDateTime, Integer> reduce =
        result.reduce(
            (box1, box2) -> {
              return new Tuple4<>(
                  box1._1().union(box2._1()),
                  box1._2().isBefore(box2._2()) ? box1._2() : box2._2(),
                  box1._3().isAfter(box2._3()) ? box1._3() : box2._3(),
                  box1._4() + box2._4());
            });
    this.boundingBox = reduce._1();
    this.timeLine = new TimeLine(reduce._2(), reduce._3());
    this.dataCount = reduce._4();
  }

  @Override
  public String toString() {
    return "SetMeta{"
        + "start_time="
        + start_time
        + ", srid="
        + srid
        + ", boundingBox="
        + boundingBox
        + ", timeLine="
        + timeLine
        + ", dataCount="
        + dataCount
        + '}';
  }
}
