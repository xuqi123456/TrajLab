package whu.edu.cn.trajlab.application.tracluster.segment;

import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.GeoUtils;
import whu.edu.cn.trajlab.core.common.constant.PreProcessDefaultConstant;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2024/02/29
 */
public class SegmentUtils implements Serializable {

  public static Trajectory genNewTrajectory(
      String tid,
      String oid,
      List<TrajPoint> pointList,
      Map<String, Object> extendedValue,
      double minTrajLength) {
    if (GeoUtils.getTrajListLen(pointList) < minTrajLength
        || pointList.size() < PreProcessDefaultConstant.DEFAULT_MIN_TRAJECTORY_NUM) {
      return null;
    } else {
      long timeHash = pointList.get(0).getTimestamp().toEpochSecond();
      return new Trajectory(tid + "_" + timeHash, oid, pointList, extendedValue);
    }
  }
}
