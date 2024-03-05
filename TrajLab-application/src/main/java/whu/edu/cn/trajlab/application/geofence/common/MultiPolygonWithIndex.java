package whu.edu.cn.trajlab.application.geofence.common;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;

import java.util.Arrays;

/**
 * @author xuqi
 * @date 2024/03/04
 */
public class MultiPolygonWithIndex extends MultiPolygon {
    private final PolygonWithIndex[] polygons;

    public MultiPolygonWithIndex(Polygon[] polygons,
                                 GeometryFactory factory) {
        super(polygons, factory);
        this.polygons = new PolygonWithIndex[polygons.length];
        for (int i = 0; i < polygons.length; ++i) {
            this.polygons[i] = PolygonWithIndex.fromPolygon(polygons[i]);
        }
    }

    public static MultiPolygonWithIndex fromMultiPolygon(MultiPolygon multiPolygon) {
        Polygon[] polygons = new Polygon[multiPolygon.getNumGeometries()];
        Arrays.setAll(polygons, multiPolygon::getGeometryN);
        MultiPolygonWithIndex multiPolygonWithIndex =
                new MultiPolygonWithIndex(polygons, multiPolygon.getFactory());
        multiPolygonWithIndex.setUserData(multiPolygon.getUserData());
        return multiPolygonWithIndex;
    }

    @Override
    public boolean intersects(Geometry g) {
        for (PolygonWithIndex polygon : polygons) {
            if (polygon.intersects(g)) {
                return true;
            }
        }
        return false;
    }
}
