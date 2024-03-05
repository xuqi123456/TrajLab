package whu.edu.cn.trajlab.application.geofence.common;

import org.locationtech.jts.geom.*;
import whu.edu.cn.trajlab.core.common.index.STRTreeIndex;
import whu.edu.cn.trajlab.core.common.index.TreeIndex;

import java.util.Arrays;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/04
 */
public final class PolygonWithIndex extends Polygon {
    private final TreeIndex<LineString> edges = new STRTreeIndex<>();

    public PolygonWithIndex(LinearRing shell,
                            LinearRing[] holes,
                            GeometryFactory factory) {
        super(shell, holes, factory);
        // create R-Tree Index for edges
        Coordinate[] coordinates = this.getCoordinates();
        for (int i = 0; i < coordinates.length; ++i) {
            if (i == coordinates.length - 1) {
                edges.insert(factory.createLineString(new Coordinate[] {coordinates[i], coordinates[0]}));
            } else {
                edges.insert(
                        factory.createLineString(new Coordinate[] {coordinates[i], coordinates[i + 1]}));
            }
        }
    }

    public static PolygonWithIndex fromPolygon(Polygon polygon) {
        LinearRing shell = (LinearRing) polygon.getExteriorRing();
        LinearRing[] holes = new LinearRing[polygon.getNumInteriorRing()];
        Arrays.setAll(holes, polygon::getInteriorRingN);
        PolygonWithIndex polygonWithIndex = new PolygonWithIndex(shell, holes, polygon.getFactory());
        polygonWithIndex.setUserData(polygon.getUserData());
        return polygonWithIndex;
    }

    @Override
    public boolean intersects(Geometry g) {
        List<LineString> candidateLineString = edges.query(g.getEnvelopeInternal());
        for (LineString lineString : candidateLineString) {
            if (lineString.intersects(g)) {
                return true;
            }
        }
        return false;
    }
}
