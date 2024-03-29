package whu.edu.cn.trajlab.db.database.util;

import scala.Tuple2;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.base.mbr.MinimumBoundingBox;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.TrajFeatures;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.db.constant.DBConstants;

import java.io.IOException;
import java.io.Serializable;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;


/**
 * Utils helps serialize/deserialize trajectory objects to/from HBase Put/Result.
 * @author xuqi
 * @date 2023/12/03
 */
public class TrajectorySerdeUtils {

    /**
     * Get an main index, so TRAJ_POINTS is not null, but PTR is null
     */
    public static Put getPutForMainIndex(IndexMeta indexMeta, Trajectory trajectory)
            throws IOException {
        Put put = addBasicTrajectoryInfos(indexMeta, trajectory);
        put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER,
                SerializerUtils.serializeObject(trajectory.getObjectID()));
        put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER,
                SerializerUtils.serializeObject(trajectory.getTrajectoryID()));
        put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.TRAJ_POINTS_QUALIFIER,
                SerializerUtils.serializeList(trajectory.getPointList(), TrajPoint.class));
        return put;
    }

    /**
     * Get a secondary index put, so TRAJ_POINTS is null, but PTR points to a main index row key byte
     * array.
     */
    public static Put getPutForSecondaryIndex(IndexMeta indexMeta, Trajectory trajectory, byte[] ptr,  boolean includePreFilterColumns) throws IOException {
        Put put = new Put(indexMeta.getIndexStrategy().index(trajectory).getBytes());
        if (!includePreFilterColumns) {
            put = addBasicTrajectoryInfos(indexMeta, trajectory);
        }
        put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.PTR_QUALIFIER, ptr);
        return put;
    }

    public static Trajectory getTrajectoryFromPut(Put p) throws IOException {
        byte[] trajPointByteArray = CellUtil.cloneValue(
                p.get(DBConstants.COLUMN_FAMILY, DBConstants.TRAJ_POINTS_QUALIFIER).get(0));
        byte[] objectID = CellUtil.cloneValue(p.get(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER).get(0));
        byte[] tID = CellUtil.cloneValue(p.get(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER).get(0));
        Trajectory trajectory = bytesToTrajectory(trajPointByteArray, objectID, tID);
        return trajectory;
    }

    public static Trajectory getTrajectoryFromResult(Result result) throws IOException {
        byte[] trajPointByteArray = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.TRAJ_POINTS_QUALIFIER);
        byte[] objectID = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER);
        byte[] tID = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER);
        return bytesToTrajectory(trajPointByteArray, objectID, tID);
    }

    public static Trajectory getAllTrajectoryFromResult(Result result) throws IOException {
        byte[] trajPointByteArray = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.TRAJ_POINTS_QUALIFIER);
        byte[] objectID = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER);
        byte[] tID = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER);
        Trajectory trajectory = bytesToTrajectory(trajPointByteArray, objectID, tID);
        addFeaturesToTrajectory(trajectory, result);
        return trajectory;
    }

    public static void addFeaturesToTrajectory(Trajectory trajectory, Result result) {

        if (result.containsColumn(DBConstants.COLUMN_FAMILY, DBConstants.START_TIME_QUALIFIER)) {
            TrajFeatures trajectoryFeaturesFromResult = getTrajectoryFeaturesFromResult(result);
            trajectory.setTrajectoryFeatures(trajectoryFeaturesFromResult);
        }
        if (result.containsColumn(DBConstants.COLUMN_FAMILY, DBConstants.EXT_VALUES_QUALIFIER)) {
            byte[] extendValue = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.EXT_VALUES_QUALIFIER);
            HashMap<String, Object> extendValueStr =
                    (HashMap<String, Object>) SerializerUtils.deserializeObject(extendValue,
                            HashMap.class);
            trajectory.setExtendedValues(extendValueStr);
        }
    }

    private static TrajFeatures getTrajectoryFeaturesFromResult(Result result) {
        byte[] startTime = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.START_TIME_QUALIFIER);
        byte[] endTime = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.END_TIME_QUALIFIER);
        byte[] startPoint = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.START_POINT_QUALIFIER);
        byte[] endPoint = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.END_POINT_QUALIFIER);
        byte[] pointNum = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.POINT_NUMBER_QUALIFIER);
        byte[] mbr = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.MBR_QUALIFIER);
        byte[] speed = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.SPEED_QUALIFIER);
        byte[] length = result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.LENGTH_QUALIFIER);
        return bytesToTrajFeatures(startTime, endTime, startPoint, endPoint,
                pointNum, mbr, speed, length);
    }

    private static TrajFeatures bytesToTrajFeatures(byte[] startTime, byte[] endTime,
                                                    byte[] startPoint, byte[] endPoint,
                                                    byte[] pointNum, byte[] mbr, byte[] speed,
                                                    byte[] length) {
        ZonedDateTime startTimeStr =
                (ZonedDateTime) SerializerUtils.deserializeObject(startTime, ZonedDateTime.class);
        ZonedDateTime endTimeStr =
                (ZonedDateTime) SerializerUtils.deserializeObject(endTime, ZonedDateTime.class);
        TrajPoint startPointStr = (TrajPoint) SerializerUtils.deserializeObject(startPoint,
                TrajPoint.class);
        TrajPoint endPointStr = (TrajPoint) SerializerUtils.deserializeObject(endPoint,
                TrajPoint.class);
        Integer pointNumStr = (Integer) SerializerUtils.deserializeObject(pointNum, Integer.class);
        MinimumBoundingBox mbrStr =
                (MinimumBoundingBox) SerializerUtils.deserializeObject(mbr, MinimumBoundingBox.class);
        Double speedStr = (Double) SerializerUtils.deserializeObject(speed, Double.class);
        Double lengthStr = (Double) SerializerUtils.deserializeObject(length, Double.class);
        return new TrajFeatures(startTimeStr, endTimeStr, startPointStr, endPointStr, pointNumStr,
                mbrStr,
                speedStr, lengthStr);
    }

    private static Trajectory bytesToTrajectory(byte[] trajPointByteArray, byte[] objectID,
                                                byte[] tID) throws IOException {
        List<TrajPoint> trajPointList = SerializerUtils.deserializeList(trajPointByteArray,
                TrajPoint.class);
        String objectStr = (String) SerializerUtils.deserializeObject(objectID, String.class);
        String tidStr = (String) SerializerUtils.deserializeObject(tID, String.class);
        return new Trajectory(tidStr, objectStr, trajPointList);
    }

    /**
     * Add basic columns (except for TRAJ_POINTS, PTR, SIGNATURE) into put object
     */
    private static Put addBasicTrajectoryInfos(IndexMeta indexMeta, Trajectory trajectory)
            throws IOException {
        Put put = new Put(indexMeta.getIndexStrategy().index(trajectory).getBytes());
        if (!trajectory.isUpdateFeatures()) {
            TrajFeatures trajectoryFeatures = trajectory.getTrajectoryFeatures();
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.MBR_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getMbr()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.START_POINT_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getStartPoint()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.END_POINT_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getEndPoint()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.START_TIME_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getStartTime()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.END_TIME_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getEndTime()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.POINT_NUMBER_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getPointNum()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.SPEED_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getSpeed()));
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.LENGTH_QUALIFIER,
                    SerializerUtils.serializeObject(trajectoryFeatures.getLen()));
        }
        if (trajectory.getExtendedValues() != null) {
            put.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.EXT_VALUES_QUALIFIER,
                    SerializerUtils.serializeObject((Serializable) trajectory.getExtendedValues()));
        }
        return put;
    }

    /**
     * 将<b>主索引表</b>中的行转换为轨迹对象
     *
     * @param result 主索引表的行，包含了轨迹的全部信息
     * @return 该行存储的轨迹对象
     */
    public static Trajectory mainRowToTrajectory(Result result) throws IOException {
        Trajectory trajectory = new Trajectory();
        trajectory.setTrajectoryID((String) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER), String.class));
        trajectory.setObjectID((String) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER), String.class));
        trajectory.setPointList(
                SerializerUtils.deserializeList(result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.TRAJ_POINTS_QUALIFIER),
                        TrajPoint.class));
        return trajectory;
    }

    /**
     * 直接获取Result对象中的MBR列
     */
    public static MinimumBoundingBox getTrajectoryMBR(Result result) throws IOException {
        return (MinimumBoundingBox) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.MBR_QUALIFIER), MinimumBoundingBox.class);
    }

    public static TimeLine getTrajectoryTimeLine(Result result) {
        ZonedDateTime startTime = ((TrajPoint) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.START_POINT_QUALIFIER), TrajPoint.class)).getTimestamp();
        ZonedDateTime endTime = ((TrajPoint) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.END_POINT_QUALIFIER), TrajPoint.class)).getTimestamp();
        return new TimeLine(startTime, endTime);
    }
    public static Tuple2<TrajPoint, TrajPoint> getTrajectorySEPoint(Result result) {
        TrajPoint startPoint = ((TrajPoint) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.START_POINT_QUALIFIER), TrajPoint.class));
        TrajPoint endPoint = ((TrajPoint) SerializerUtils.deserializeObject(
                result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.END_POINT_QUALIFIER), TrajPoint.class));
        return new Tuple2<TrajPoint, TrajPoint>(startPoint, endPoint);
    }

    public static byte[] getByteArrayByQualifier(Result result, byte[] qualifier) {
        return result.getValue(DBConstants.COLUMN_FAMILY, qualifier);
    }

    public static boolean isMainIndexed(Result result) {
        return result.getValue(DBConstants.COLUMN_FAMILY, DBConstants.PTR_QUALIFIER) == null;
    }
}
