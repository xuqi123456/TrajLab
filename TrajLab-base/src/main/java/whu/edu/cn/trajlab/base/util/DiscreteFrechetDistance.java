package whu.edu.cn.trajlab.base.util;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class DiscreteFrechetDistance {
    private static final GeometryFactory factory = JTSFactoryFinder.getGeometryFactory();

    public static double calculateDFD(Geometry geom1, Geometry geom2) {
        Coordinate[] coords1 = geom1.getCoordinates();
        Coordinate[] coords2 = geom2.getCoordinates();

        double[][] distanceMatrix = new double[coords1.length][coords2.length];
        for (int i = 0; i < coords1.length; i++) {
            for (int j = 0; j < coords2.length; j++) {
                distanceMatrix[i][j] = -1;
            }
        }
        for (int i = 0; i < coords1.length; i++) {
            distanceMatrix[i][0] = Math.max(distanceMatrix[i][0], GeoUtils.getEuclideanDistanceKM(coords1[i],coords2[0]));
        }
        for (int i = 0; i < coords2.length; i++) {
            distanceMatrix[0][i] = Math.max(distanceMatrix[0][i], GeoUtils.getEuclideanDistanceKM(coords2[i],coords1[0]));
        }
        for (int i = 1; i < coords1.length; i++) {
            for (int j = 1; j < coords2.length; j++) {
                double prevDist1 = distanceMatrix[i - 1][j];
                double prevDist2 = distanceMatrix[i - 1][j-1];
                double prevDist3 = distanceMatrix[i][j-1];
                double curDist = GeoUtils.getEuclideanDistanceKM(coords1[i], coords2[j]);
                distanceMatrix[i][j] = Math.max(curDist, Math.min(Math.min(prevDist1, prevDist2),prevDist3));
            }
        }
        return distanceMatrix[coords1.length-1][coords2.length-1];
    }

    public static void main(String[] args) {
        // 示例用法

        Coordinate[] coords1 = new Coordinate[] {new Coordinate(0, 0), new Coordinate(1, 1)};
        Coordinate[] coords2 = new Coordinate[] {new Coordinate(0, 1), new Coordinate(1, 2),new Coordinate(1, 3)};
        Geometry geom1 = factory.createLineString(coords1);
        Geometry geom2 = factory.createLineString(coords2);

        double dfd = calculateDFD(geom1, geom2);
        System.out.println("The Discrete Fréchet Distance between geom1 and geom2 is: " + dfd);
    }
}
