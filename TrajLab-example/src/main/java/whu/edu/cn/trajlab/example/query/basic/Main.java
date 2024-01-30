package whu.edu.cn.trajlab.example.query.basic;


import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.algorithm.distance.DistanceToPoint;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.distance.DistanceOp;
import whu.edu.cn.trajlab.base.util.GeoUtils;

/**
 * @author xuqi
 * @date 2024/01/30
 */
public class Main {
    public static void main(String[] args) {
        // 创建一个点和一个多边形
        GeometryFactory factory = new GeometryFactory();
        Point point = factory.createPoint(new Coordinate(1.4, 3));
        Geometry polygon = factory.createPolygon(new Coordinate[]{
                new Coordinate(0, 0),
                new Coordinate(0, 2),
                new Coordinate(2, 2),
                new Coordinate(2, 0),
                new Coordinate(0, 0)
        });

        // 计算点到多边形边界的最短距离及对应的多边形上的两个点
        DistanceOp distanceOp = new DistanceOp(point, polygon);
        double minDistance = distanceOp.distance();
        Coordinate[] closestCoords = distanceOp.nearestPoints();
        double v = GeoUtils.nearDistanceOp(point, polygon);
    System.out.println(v);
    }
}
