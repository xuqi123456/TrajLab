package whu.edu.cn.trajlab.application.geofence;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import scala.Serializable;
import scala.Tuple2;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.common.index.TreeIndex;
import whu.edu.cn.trajlab.core.enums.TopologyTypeEnum;
import whu.edu.cn.trajlab.core.util.EvaluatorUtils;

import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/04
 */
public class Geofence<T extends Geometry> implements Serializable {
    private EvaluatorUtils.SpatialPredicateEvaluator evaluator;

    public Geofence() {
        evaluator = EvaluatorUtils.create(TopologyTypeEnum.INTERSECTS);
    }

    public Tuple2<String, String> geofence(Trajectory traj, TreeIndex<T> treeIndex) {
        LineString trajLine = traj.getLineString();
        List<T> result = treeIndex.query(trajLine);
        if (result.size() == 0) {
            return null;
        }
        for (T polygon : result) {
            if (evaluator.eval(polygon, trajLine)) {
                return new Tuple2<>(traj.getObjectID() + "-" + traj.getTrajectoryID(),
                        (String) polygon.getUserData());
            }
        }
        return null;
    }
}
