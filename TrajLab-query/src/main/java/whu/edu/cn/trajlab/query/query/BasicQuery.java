package whu.edu.cn.trajlab.query.query;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import scala.NotImplementedError;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.*;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.query.query.basic.*;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;
import static whu.edu.cn.trajlab.query.query.QueryConf.*;

public class BasicQuery extends Configured {

    public BasicQuery(Configuration conf) {
        this.setConf(conf);
    }

    public List<Trajectory> getScanQuery() throws IOException, ParseException {
        Configuration conf = getConf();
        String indextype = conf.get(INDEX_TYPE);
        String dataset_name = conf.get(DATASET_NAME);
        switch (IndexType.valueOf(indextype)) {
            case XZ2:{
                Database instance = Database.getInstance();
                WKTReader wktReader = new WKTReader();
                String spatialQueryWindow = conf.get(SPATIAL_WINDOW);
                Geometry envelopeIntersect = wktReader.read(spatialQueryWindow);
                SpatialQueryCondition spatialCondition = new SpatialQueryCondition(
                        envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
                SpatialQuery spatialQuery =
                        new SpatialQuery(instance.getDataSet(dataset_name), spatialCondition);
                return spatialQuery.executeQuery();
            }
            case TXZ2:
            case XZ2T: {
                Database instance = Database.getInstance();
                WKTReader wktReader = new WKTReader();
                String spatialQueryWindow = conf.get(SPATIAL_WINDOW);
                Geometry envelopeIntersect = wktReader.read(spatialQueryWindow);
                SpatialQueryCondition spatialCondition = new SpatialQueryCondition(
                        envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);

                List<TimeLine> timeLineList = new ArrayList<>();
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
                ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
                ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
                TimeLine testTimeLine = new TimeLine(start, end);
                timeLineList.add(testTimeLine);
                TemporalQueryCondition temporalCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
                SpatialTemporalQueryCondition stQueryConditionIntersect = new SpatialTemporalQueryCondition(
                        spatialCondition, temporalCondition);
                SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(dataset_name),
                        stQueryConditionIntersect);
                return spatialTemporalQuery.executeQuery();
            }
            case OBJECT_ID_T: {
                Database instance = Database.getInstance();
                IDQueryCondition idQueryCondition = new IDQueryCondition(conf.get(OID));
                List<TimeLine> timeLineList = new ArrayList<>();
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
                ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
                ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
                TimeLine testTimeLine = new TimeLine(start, end);
                timeLineList.add(testTimeLine);
                TemporalQueryCondition temporalCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
                IDTemporalQueryCondition idTemporalQueryCondition = new IDTemporalQueryCondition(temporalCondition, idQueryCondition);
                DataSet dataSet = instance.getDataSet(dataset_name);
                IDTemporalQuery iDTemporalQuery = new IDTemporalQuery(dataSet, idTemporalQueryCondition);
                return iDTemporalQuery.executeQuery();
            }
            case Temporal: {
                Database instance = Database.getInstance();
                List<TimeLine> timeLineList = new ArrayList<>();
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
                ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
                ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
                TimeLine testTimeLine = new TimeLine(start, end);
                timeLineList.add(testTimeLine);
                TemporalQueryCondition temporalCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
                DataSet dataSet = instance.getDataSet(dataset_name);
                TemporalQuery temporalQuery = new TemporalQuery(dataSet, temporalCondition);
                return temporalQuery.executeQuery();
            }
            case ID:{
                Database instance = Database.getInstance();
                DataSet dataSet = instance.getDataSet(dataset_name);
                IDQueryCondition idQueryCondition = new IDQueryCondition(conf.get(OID));
                IDQuery idQuery = new IDQuery(dataSet, idQueryCondition);
                return idQuery.executeQuery();
            }
            default:
                throw new NotImplementedError();
        }
    }

    public JavaRDD<Trajectory> getRDDScanQuery(SparkSession sparkSession) throws IOException, ParseException {
        Configuration conf = getConf();
        String indextype = conf.get(INDEX_TYPE);
        String dataset_name = conf.get(DATASET_NAME);
        switch (IndexType.valueOf(indextype)) {
            case XZ2:{
                Database instance = Database.getInstance();
                WKTReader wktReader = new WKTReader();
                String spatialQueryWindow = conf.get(SPATIAL_WINDOW);
                Geometry envelopeIntersect = wktReader.read(spatialQueryWindow);
                SpatialQueryCondition spatialCondition = new SpatialQueryCondition(
                        envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);
                SpatialQuery spatialQuery =
                        new SpatialQuery(instance.getDataSet(dataset_name), spatialCondition);
               return spatialQuery.getRDDQuery(sparkSession);
            }
            case TXZ2:
            case XZ2T: {
                Database instance = Database.getInstance();
                WKTReader wktReader = new WKTReader();
                String spatialQueryWindow = conf.get(SPATIAL_WINDOW);
                Geometry envelopeIntersect = wktReader.read(spatialQueryWindow);
                SpatialQueryCondition spatialCondition = new SpatialQueryCondition(
                        envelopeIntersect, SpatialQueryCondition.SpatialQueryType.INTERSECT);

                List<TimeLine> timeLineList = new ArrayList<>();
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
                ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
                ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
                TimeLine testTimeLine = new TimeLine(start, end);
                timeLineList.add(testTimeLine);
                TemporalQueryCondition temporalCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
                SpatialTemporalQueryCondition stQueryConditionIntersect = new SpatialTemporalQueryCondition(
                        spatialCondition, temporalCondition);
                SpatialTemporalQuery spatialTemporalQuery = new SpatialTemporalQuery(instance.getDataSet(dataset_name),
                        stQueryConditionIntersect);
                return spatialTemporalQuery.getRDDQuery(sparkSession);
            }
            case OBJECT_ID_T: {
                Database instance = Database.getInstance();
                IDQueryCondition idQueryCondition = new IDQueryCondition(conf.get(OID));
                List<TimeLine> timeLineList = new ArrayList<>();
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
                ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
                ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
                TimeLine testTimeLine = new TimeLine(start, end);
                timeLineList.add(testTimeLine);
                TemporalQueryCondition temporalCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
                IDTemporalQueryCondition idTemporalQueryCondition = new IDTemporalQueryCondition(temporalCondition, idQueryCondition);
                DataSet dataSet = instance.getDataSet(dataset_name);
                IDTemporalQuery iDTemporalQuery = new IDTemporalQuery(dataSet, idTemporalQueryCondition);
                return iDTemporalQuery.getRDDQuery(sparkSession);
            }
            case Temporal: {
                Database instance = Database.getInstance();
                List<TimeLine> timeLineList = new ArrayList<>();
                DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
                ZonedDateTime start = ZonedDateTime.parse(conf.get(START_TIME), dateTimeFormatter);
                ZonedDateTime end = ZonedDateTime.parse(conf.get(END_TIME), dateTimeFormatter);
                TimeLine testTimeLine = new TimeLine(start, end);
                timeLineList.add(testTimeLine);
                TemporalQueryCondition temporalCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
                DataSet dataSet = instance.getDataSet(dataset_name);
                TemporalQuery temporalQuery = new TemporalQuery(dataSet, temporalCondition);
                return temporalQuery.getRDDQuery(sparkSession);
            }
            case ID:{
                Database instance = Database.getInstance();
                DataSet dataSet = instance.getDataSet(dataset_name);
                IDQueryCondition idQueryCondition = new IDQueryCondition(conf.get(OID));
                IDQuery idQuery = new IDQuery(dataSet, idQueryCondition);
                return idQuery.getRDDQuery(sparkSession);
            }
            default:
                throw new NotImplementedError();
        }
    }
}
