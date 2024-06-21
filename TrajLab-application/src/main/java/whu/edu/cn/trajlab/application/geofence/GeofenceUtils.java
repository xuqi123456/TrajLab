package whu.edu.cn.trajlab.application.geofence;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.application.geofence.common.MultiPolygonWithIndex;
import whu.edu.cn.trajlab.application.geofence.common.PolygonWithIndex;
import whu.edu.cn.trajlab.core.common.index.STRTreeIndex;
import whu.edu.cn.trajlab.core.util.FSUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/04
 */
public class GeofenceUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GeofenceUtils.class);
    public static GeometryFactory geometryFactory = new GeometryFactory();
    public static WKTReader wktReader = new WKTReader(geometryFactory);
    private static final int MAX_WKT_LENGTH = 32767;
    private static final int MIN_INDEXED_NUM_POINTS = 32;
    public static List<Geometry> readBeijingDistricts(String path) {
        List<Geometry> polygons = new ArrayList<>(16);
        File file = new File(path);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] items = line.split("\t");
                String id = items[0];
                Geometry polygon = wktReader.read(items[2]);
                polygon.setUserData(id);
                polygons.add(polygon);
            }
        } catch (IOException e) {
            LOGGER.error("Cannot read Beijing district file from {}", path, e);
        } catch (ParseException e) {
            LOGGER.error("Parse exception", e);
        }
        return polygons;
    }

    public static List<Geometry> readGeoFence(String path) {
        List<Geometry> polygons = new ArrayList<>(16);
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
                Geometry polygon = wktReader.read(wkt);
                polygon.setUserData(id);
                polygons.add(polygon);
            }
        } catch (IOException e) {
            LOGGER.error("Cannot read file from {}", path, e);
        } catch (ParseException e) {
            LOGGER.error("Parse exception", e);
        }
        return polygons;
    }

    public static List<Geometry> readGeoFence(String fs, String path) {
        String content = FSUtils.readFromFS(fs, path);
        List<Geometry> polygons = new ArrayList<>(16);
        int idx = 0;
        assert content != null;
        String[] lines = content.split(System.lineSeparator());
        for (String line : lines) {
            if (idx == 0) {
                idx++;
                continue;
            }
            try {
                String[] items = line.split(",");
                String id = line.split(",")[0];
                String wkt = line.split("\"")[1];
                if (wkt.length() >= MAX_WKT_LENGTH) {
                    continue;
                }
                Geometry polygon = wktReader.read(wkt);
                polygon.setUserData(id);
                polygons.add(polygon);
            } catch (Exception e) {
                LOGGER.error("Parse exception", e);
            }
        }
        return polygons;
    }

    public static STRTreeIndex<Geometry> getIndexedGeoFence(List<Geometry> geofenceList,
                                                            boolean indexEdge) {
        STRTreeIndex<Geometry> treeIndex = new STRTreeIndex<Geometry>();
        for (Geometry geometry : geofenceList) {
            if (indexEdge && geometry instanceof Polygon && geometry.getNumPoints() > MIN_INDEXED_NUM_POINTS) {
                treeIndex.insert(PolygonWithIndex.fromPolygon((Polygon) geometry));
            } else if (indexEdge && geometry instanceof MultiPolygonWithIndex && geometry.getNumPoints() > MIN_INDEXED_NUM_POINTS) {
                treeIndex.insert(MultiPolygonWithIndex.fromMultiPolygon((MultiPolygonWithIndex) geometry));
            } else {
                treeIndex.insert(geometry);
            }
        }
        return treeIndex;
    }

    public static STRTreeIndex<Geometry> getIndexedGeoFence(String fs, String path,
                                                            boolean indexEdge) {
        List<Geometry> geofenceList = readGeoFence(fs, path);
        return getIndexedGeoFence(geofenceList, indexEdge);
    }

    public static STRTreeIndex<Geometry> getIndexedGeoFence(String path) {
        List<Geometry> geofenceList = readGeoFence(path);
        return getIndexedGeoFence(geofenceList, false);
    }
}
