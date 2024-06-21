package whu.edu.cn.trajlab.query.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.io.ParseException;
import scala.NotImplementedError;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.db.condition.*;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.db.enums.TimePeriod;
import whu.edu.cn.trajlab.query.query.advanced.AccompanyQuery;
import whu.edu.cn.trajlab.query.query.advanced.BufferQuery;
import whu.edu.cn.trajlab.query.query.advanced.KNNQuery;
import whu.edu.cn.trajlab.query.query.advanced.SimilarQuery;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;
import static whu.edu.cn.trajlab.query.query.QueryConf.*;

public class AdvancedQuery extends Configured {

  public AdvancedQuery(Configuration conf) {
    this.setConf(conf);
  }

  public List<Trajectory> getScanQuery() throws IOException {
    Configuration conf = getConf();
    String indextype = conf.get(INDEX_TYPE);
    String dataset_name = conf.get(DATASET_NAME);
    switch (IndexType.valueOf(indextype)) {
      case KNN:
        {
          Database instance = Database.getInstance();
          if (conf.get(CENTER_POINT) != null) {
            String center_PointBytes = conf.get(CENTER_POINT);
            byte[] center_Point = Base64.getDecoder().decode(center_PointBytes);
            TrajPoint centralPoint =
                (TrajPoint) SerializerUtils.deserializeObject(center_Point, TrajPoint.class);
            KNNQueryCondition knnQueryCondition;
            if (conf.get(START_TIME) != null) {
              List<TimeLine> timeLineList = new ArrayList<>();
              DateTimeFormatter dateTimeFormatter =
                  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
              ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
              ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
              TimeLine testTimeLine = new TimeLine(start, end);
              timeLineList.add(testTimeLine);
              TemporalQueryCondition temporalCondition =
                  new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
              knnQueryCondition =
                  new KNNQueryCondition(
                      Integer.parseInt(conf.get(K)), centralPoint, temporalCondition);
            } else {
              knnQueryCondition =
                  new KNNQueryCondition(Integer.parseInt(conf.get(K)), centralPoint);
            }
            KNNQuery knnQuery = new KNNQuery(instance.getDataSet(dataset_name), knnQueryCondition);
            return knnQuery.executeQuery();
          } else {
            String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
            byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
            Trajectory centralTraj =
                (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
            KNNQueryCondition knnQueryCondition;
            if (conf.get(START_TIME) != null) {
              List<TimeLine> timeLineList = new ArrayList<>();
              DateTimeFormatter dateTimeFormatter =
                  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
              ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
              ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
              TimeLine testTimeLine = new TimeLine(start, end);
              timeLineList.add(testTimeLine);
              TemporalQueryCondition temporalCondition =
                  new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
              knnQueryCondition =
                  new KNNQueryCondition(
                      Integer.parseInt(conf.get(K)), centralTraj, temporalCondition);
            } else {
              knnQueryCondition = new KNNQueryCondition(Integer.parseInt(conf.get(K)), centralTraj);
            }
            KNNQuery knnQuery = new KNNQuery(instance.getDataSet(dataset_name), knnQueryCondition);
            return knnQuery.executeQuery();
          }
        }
      case BUFFER:
        {
          Database instance = Database.getInstance();
          String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
          byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
          Trajectory centralTraj =
              (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
          BufferQueryConditon bqc =
              new BufferQueryConditon(centralTraj, Double.parseDouble(conf.get(DIS_THRESHOLD)));
          BufferQuery bufferQuery = new BufferQuery(instance.getDataSet(dataset_name), bqc);
          return bufferQuery.executeQuery();
        }
      case SIMILAR:
        {
          Database instance = Database.getInstance();
          String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
          byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
          Trajectory centralTraj =
              (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
          SimilarQueryCondition sqc;
          if (conf.get(START_TIME) != null) {
            List<TimeLine> timeLineList = new ArrayList<>();
            DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
            ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
            ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
            TimeLine testTimeLine = new TimeLine(start, end);
            timeLineList.add(testTimeLine);
            TemporalQueryCondition temporalCondition =
                new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
            sqc =
                new SimilarQueryCondition(
                    centralTraj, Double.parseDouble(conf.get(DIS_THRESHOLD)), temporalCondition);
          } else {
            sqc =
                new SimilarQueryCondition(centralTraj, Double.parseDouble(conf.get(DIS_THRESHOLD)));
          }
          SimilarQuery similarQuery = new SimilarQuery(instance.getDataSet(dataset_name), sqc);
          return similarQuery.executeQuery();
        }
      case ACCOMPANY:
        {
          Database instance = Database.getInstance();
          String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
          byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
          Trajectory centralTraj =
              (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
          AccompanyQueryCondition aqc =
              new AccompanyQueryCondition(
                  centralTraj,
                  Double.parseDouble(conf.get(DIS_THRESHOLD)),
                  Double.parseDouble(conf.get(TIME_THRESHOLD)),
                  Integer.parseInt(conf.get(K)));
          AccompanyQuery accompanyQuery =
              new AccompanyQuery(instance.getDataSet(dataset_name), aqc);
          return accompanyQuery.executeQuery();
        }
      default:
        throw new NotImplementedError();
    }
  }

  public JavaRDD<Trajectory> getRDDScanQuery(SparkSession sparkSession)
      throws IOException, ParseException {
    Configuration conf = getConf();
    String indextype = conf.get(INDEX_TYPE);
    String dataset_name = conf.get(DATASET_NAME);
    switch (IndexType.valueOf(indextype)) {
      case KNN:
        {
          Database instance = Database.getInstance();
          if (conf.get(CENTER_POINT) != null) {
            String center_PointBytes = conf.get(CENTER_POINT);
            byte[] center_Point = Base64.getDecoder().decode(center_PointBytes);
            TrajPoint centralPoint =
                (TrajPoint) SerializerUtils.deserializeObject(center_Point, TrajPoint.class);
            KNNQueryCondition knnQueryCondition;
            if (conf.get(START_TIME) != null) {
              List<TimeLine> timeLineList = new ArrayList<>();
              DateTimeFormatter dateTimeFormatter =
                  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
              ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
              ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
              TimeLine testTimeLine = new TimeLine(start, end);
              timeLineList.add(testTimeLine);
              TemporalQueryCondition temporalCondition =
                  new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
              knnQueryCondition =
                  new KNNQueryCondition(
                      Integer.parseInt(conf.get(K)), centralPoint, temporalCondition);
            } else {
              knnQueryCondition =
                  new KNNQueryCondition(Integer.parseInt(conf.get(K)), centralPoint);
            }
            KNNQuery knnQuery = new KNNQuery(instance.getDataSet(dataset_name), knnQueryCondition);
            return knnQuery.getRDDQuery(sparkSession);
          } else {
            String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
            byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
            Trajectory centralTraj =
                (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
            KNNQueryCondition knnQueryCondition;
            if (conf.get(START_TIME) != null) {
              List<TimeLine> timeLineList = new ArrayList<>();
              DateTimeFormatter dateTimeFormatter =
                  DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
              ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
              ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
              TimeLine testTimeLine = new TimeLine(start, end);
              timeLineList.add(testTimeLine);
              TemporalQueryCondition temporalCondition =
                  new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
              knnQueryCondition =
                  new KNNQueryCondition(
                      Integer.parseInt(conf.get(K)), centralTraj, temporalCondition);
            } else {
              knnQueryCondition = new KNNQueryCondition(Integer.parseInt(conf.get(K)), centralTraj);
            }
            KNNQuery knnQuery = new KNNQuery(instance.getDataSet(dataset_name), knnQueryCondition);
            return knnQuery.getRDDQuery(sparkSession);
          }
        }
      case BUFFER:
        {
          Database instance = Database.getInstance();
          String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
          byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
          Trajectory centralTraj =
              (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
          BufferQueryConditon bqc =
              new BufferQueryConditon(centralTraj, Double.parseDouble(conf.get(DIS_THRESHOLD)));
          BufferQuery bufferQuery = new BufferQuery(instance.getDataSet(dataset_name), bqc);
          return bufferQuery.getRDDQuery(sparkSession);
        }
      case SIMILAR:
        {
          Database instance = Database.getInstance();
          String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
          byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
          Trajectory centralTraj =
              (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
          SimilarQueryCondition sqc;
          if (conf.get(START_TIME) != null) {
            List<TimeLine> timeLineList = new ArrayList<>();
            DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
            ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
            ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
            TimeLine testTimeLine = new TimeLine(start, end);
            timeLineList.add(testTimeLine);
            TemporalQueryCondition temporalCondition =
                new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
            sqc =
                new SimilarQueryCondition(
                    centralTraj, Double.parseDouble(conf.get(DIS_THRESHOLD)), temporalCondition);
          } else {
            sqc =
                new SimilarQueryCondition(centralTraj, Double.parseDouble(conf.get(DIS_THRESHOLD)));
          }
          SimilarQuery similarQuery = new SimilarQuery(instance.getDataSet(dataset_name), sqc);
          return similarQuery.getRDDQuery(sparkSession);
        }
      case ACCOMPANY:
        {
          Database instance = Database.getInstance();
          String center_TrajBytes = conf.get(CENTER_TRAJECTORY);
          byte[] center_Traj = Base64.getDecoder().decode(center_TrajBytes);
          Trajectory centralTraj =
              (Trajectory) SerializerUtils.deserializeObject(center_Traj, Trajectory.class);
          AccompanyQueryCondition aqc =
              new AccompanyQueryCondition(
                  centralTraj,
                  Double.parseDouble(conf.get(DIS_THRESHOLD)),
                  Double.parseDouble(conf.get(TIME_THRESHOLD)),
                  TimePeriod.valueOf(conf.get(TIME_PERIOD)),
                  Integer.parseInt(conf.get(K)));
          AccompanyQuery accompanyQuery =
              new AccompanyQuery(instance.getDataSet(dataset_name), aqc);
          return accompanyQuery.getRDDQuery(sparkSession);
        }
      default:
        throw new NotImplementedError();
    }
  }
}
