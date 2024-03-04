package whu.edu.cn.trajlab.application.Tracluster.dbscan.cluster;

import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.GeometryFactory;
import whu.edu.cn.trajlab.base.point.BasePoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/03/02
 */
public class DBScanTraLine implements Serializable {

    private boolean isVisited;
    private boolean isNoise;
    private final Trajectory trajectory;

    public DBScanTraLine(Trajectory trajectory) {
        this.trajectory = trajectory;
    }

    public boolean isVisited() {
        return isVisited;
    }

    public boolean isNoise() {
        return isNoise;
    }

    public Trajectory getTrajectory() {
        return trajectory;
    }

    public void setVisited(boolean visited) {
        isVisited = visited;
    }

    public void setNoise(boolean noise) {
        isNoise = noise;
    }

    @Override
    public String toString() {
        return "DBScanTraLine{" +
                "isVisited=" + isVisited +
                ", isNoise=" + isNoise +
                ", trajectory=" + trajectory +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBScanTraLine that = (DBScanTraLine) o;
        return Objects.equals(trajectory, that.trajectory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trajectory);
    }
}
