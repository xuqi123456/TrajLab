package whu.edu.cn.trajlab.core.operator.transform.source;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.core.common.constant.TrajectoryDefaultConstant;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTReader;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/17
 */
public class WKT2Traj {
    private static final Logger LOGGER = LoggerFactory.getLogger(WKT2Traj.class);
    private static final int MAX_WKT_LENGTH = 32767;
    public static GeometryFactory geometryFactory = new GeometryFactory();
    public static WKTReader wktReader = new WKTReader(geometryFactory);
    public static List<Trajectory> parseWKTToTrajectoryList(String value) {
        WKTReader wktReader = new WKTReader();
        ArrayList<Trajectory> trajectories = new ArrayList<>();
        try {
            String[] split = value.split("\n");
            for (String str : split) {
                Geometry geometry = wktReader.read(str);
                String oid = TrajectoryDefaultConstant.oid;
                String tid = TrajectoryDefaultConstant.tid;
                ArrayList<TrajPoint> traPoints = new ArrayList<>();
                Coordinate[] coordinates = geometry.getCoordinates();
                for (int i = 0; i < coordinates.length; i++) {
                    TrajPoint trajPoint =
                            new TrajPoint(
                                    Integer.toString(i),
                                    TrajectoryDefaultConstant.DEFAULT_DATETIME,
                                    coordinates[i].x,
                                    coordinates[i].y);
                    traPoints.add(trajPoint);
                }
                trajectories.add(new Trajectory(tid, oid, traPoints));
            }
            return trajectories;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static List<Trajectory> parseWKTPathToTrajectoryList(String path){
        List<Trajectory> polygons = new ArrayList<>(16);
        int idx = 0;
        File file = new File(path);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (idx == 0) {
                    idx++;
                    continue;
                }
                String[] items = line.split(",");
                String id = line.split(",")[0];
                String wkt = line.split("\"")[1];
                if (wkt.length() >= MAX_WKT_LENGTH) {
                    continue;
                }
                Trajectory trajectory = parseWKTToTrajectory(wkt);
                if(trajectory != null){
                    polygons.add(trajectory);
                }
            }
        } catch (IOException e) {
            LOGGER.error("Cannot read file from {}", path, e);
        }
        return polygons;
    }

    public static Trajectory parseWKTToTrajectory(String value) {
        WKTReader wktReader = new WKTReader();
        String oid = TrajectoryDefaultConstant.oid;
        String tid = TrajectoryDefaultConstant.tid;
        ArrayList<TrajPoint> traPoints = new ArrayList<>();
        try {
            Geometry geometry = wktReader.read(value);
            Coordinate[] coordinates = geometry.getCoordinates();
            for (int i = 0; i < coordinates.length; i++) {
                TrajPoint trajPoint =
                        new TrajPoint(
                                Integer.toString(i),
                                TrajectoryDefaultConstant.DEFAULT_DATETIME,
                                coordinates[i].x,
                                coordinates[i].y);
                traPoints.add(trajPoint);
            }
        } catch (Exception ignored) {
        }
        if(traPoints.size() > 2){
            return new Trajectory(tid, oid, traPoints);
        }else return null;
    }
}
