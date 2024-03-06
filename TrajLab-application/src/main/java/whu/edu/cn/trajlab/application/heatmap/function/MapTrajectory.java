package whu.edu.cn.trajlab.application.heatmap.function;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * @author xuqi
 * @date 2024/03/06
 */
public class MapTrajectory {
    private static class TrajectoryMapPointFlatMap implements FlatMapFunction<Trajectory, Point> {

        @Override
        public Iterator<Point> call(Trajectory trajectory) throws Exception {
            Coordinate[] coordinates = trajectory.getLineString().getCoordinates();
            ArrayList<Point> points = new ArrayList<>();
            GeometryFactory geometryFactory = new GeometryFactory();
            for (Coordinate coordinate : coordinates) {
                Point point = geometryFactory.createPoint(coordinate);
                point.setUserData(trajectory.getTrajectoryID());
                points.add(point);
            }
            return points.iterator();
        }
    }
    public static JavaRDD<Point> FlatMapTrajectoryToPoints(JavaRDD<Trajectory> tra){
        return tra.flatMap(new TrajectoryMapPointFlatMap());
    }
}
