package whu.edu.cn.trajlab.base.trajectory;

import whu.edu.cn.trajlab.base.point.BasePoint;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.util.DiscreteFrechetDistance;
import whu.edu.cn.trajlab.base.util.GeoUtils;

/**
 * @author xuqi
 * @date 2024/01/25
 */
public class TrajectoryWithDistance {
  private double distance = -1.0;
  private Trajectory trajectory;

  public TrajectoryWithDistance(Trajectory trajectory) {
    this.trajectory = trajectory;
  }

  public TrajectoryWithDistance(Trajectory trajectory, BasePoint queryPoint) {
    this.trajectory = trajectory;
    calDistanceToPoint(queryPoint);
  }

  public TrajectoryWithDistance(Trajectory trajectory1, Trajectory trajectory2) {
    this.trajectory = trajectory1;
    calDistanceToTrajectory(trajectory1, trajectory2);
  }

  public double getDistance() {
    return distance;
  }

  public Trajectory getTrajectory() {
    return trajectory;
  }

  private void calDistanceToPoint(BasePoint p) {
    double distance = Double.MAX_VALUE;
    for (TrajPoint trajPoint : this.trajectory.getPointList()) {
      BasePoint trajBasePoint = new BasePoint(trajPoint.getLng(), trajPoint.getLat());
      double pDistance = GeoUtils.getEuclideanDistanceKM(p, trajBasePoint);
      if (pDistance < distance) {
        distance = pDistance;
      }
    }
    this.distance = distance;
  }

  private void calDistanceToTrajectory(Trajectory t1, Trajectory t2) {
    this.distance = DiscreteFrechetDistance.calculateDFD(t1.getLineString(), t2.getLineString());
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof TrajectoryWithDistance) {
      TrajectoryWithDistance trajectory = (TrajectoryWithDistance) obj;
      if (!trajectory.equals(this.trajectory)) {
        return false;
      }
      if (Math.abs(distance - trajectory.distance) > 1e-8) {
        return false;
      }
      return true;
    }
    return false;
  }
}
