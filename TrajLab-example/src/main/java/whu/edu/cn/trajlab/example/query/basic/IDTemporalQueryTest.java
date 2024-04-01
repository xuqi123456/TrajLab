package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.condition.IDTemporalQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.basic.IDTemporalQuery;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.example.load.HBaseDataLoad.getLoadHBase;
import static whu.edu.cn.trajlab.example.query.basic.IDQueryTest.moid;
import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;
import static whu.edu.cn.trajlab.example.query.basic.TemporalQueryTest.testTimeLine1;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class IDTemporalQueryTest extends TestCase {
    public static TemporalQueryCondition temporalContainCondition;
    public static TemporalQueryCondition temporalIntersectCondition;
    public static IDQueryCondition idQueryCondition = new IDQueryCondition(moid);

    static List<TimeLine> timeLineList = new ArrayList<>();
    static {
        timeLineList.add(testTimeLine1);
        temporalIntersectCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.INTERSECT);
        temporalContainCondition = new TemporalQueryCondition(timeLineList, TemporalQueryType.CONTAIN);
    }

    public void testExecuteRDDIntersectQuery() throws IOException {
        Database instance = Database.getInstance();
        IndexTable indexTable = instance.getDataSet(DATASET_NAME).getCoreIndexTable();
        IDTemporalQueryCondition idTemporalQueryCondition = new IDTemporalQueryCondition(temporalIntersectCondition, idQueryCondition);
        IDTemporalQuery idTemporalQuery =
                new IDTemporalQuery(instance.getDataSet(DATASET_NAME), idTemporalQueryCondition);
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = idTemporalQuery.getRDDQuery(sparkSession);
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
            int intersect = testGetAnswer(false);
            assertEquals(intersect, results.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void testINTERSECTQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        IDTemporalQueryCondition idTemporalQueryCondition = new IDTemporalQueryCondition(temporalIntersectCondition, idQueryCondition);
        IDTemporalQuery IDTemporalQuery = new IDTemporalQuery(dataSet, idTemporalQueryCondition);
        List<Trajectory> trajectories = IDTemporalQuery.executeQuery();
        System.out.println(trajectories.size());
        for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        int intersect = testGetAnswer(false);
        assertEquals(intersect, trajectories.size());
    }

    public void testContainQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        IDTemporalQueryCondition idTemporalQueryCondition = new IDTemporalQueryCondition(temporalContainCondition, idQueryCondition);
        IDTemporalQuery IDTemporalQuery = new IDTemporalQuery(dataSet, idTemporalQueryCondition);
        List<Trajectory> trajectories = IDTemporalQuery.executeQuery();
        System.out.println(trajectories.size());
        for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        int contain = testGetAnswer(true);
        assertEquals(contain, trajectories.size());
    }

    public int testGetAnswer(boolean isContained) throws IOException {
        List<Trajectory> trips = getLoadHBase();
        int i = 0;
        int j = 0;
        for (Trajectory trajectory : trips) {
            ZonedDateTime startTime = trajectory.getTrajectoryFeatures().getStartTime();
            ZonedDateTime endTime = trajectory.getTrajectoryFeatures().getEndTime();
            TimeLine trajTimeLine = new TimeLine(startTime, endTime);
            for (TimeLine queryTimeLine : timeLineList) {
                if (queryTimeLine.contain(trajTimeLine) && trajectory.getObjectID().equals(moid)) {
                    i++;
                }
                if (queryTimeLine.intersect(trajTimeLine) && trajectory.getObjectID().equals(moid)) {
                    j++;
                }
            }
        }
        if (isContained){
            return i;
        }else return j;
    }

    public void testDeleteDataSet() throws IOException {
        Database instance = Database.getInstance();
        instance.deleteDataSet(DATASET_NAME);
    }
}
