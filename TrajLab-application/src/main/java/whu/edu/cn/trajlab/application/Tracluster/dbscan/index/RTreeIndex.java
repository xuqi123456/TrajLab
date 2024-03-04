package whu.edu.cn.trajlab.application.Tracluster.dbscan.index;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.index.strtree.STRtree;
import whu.edu.cn.trajlab.application.Tracluster.dbscan.cluster.DBScanTraLine;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.GeoUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/03
 */
public class RTreeIndex<T extends DBScanTraLine> implements Serializable {

    private static final int DEFAULT_NODE_CAPACITY = 2;
    private final STRtree stRtree;

    public RTreeIndex() {
        stRtree = new STRtree(DEFAULT_NODE_CAPACITY);
    }

    public RTreeIndex(int nodeCapacity) {
        stRtree = new STRtree(nodeCapacity);
    }

    public void insert(List<T> geometries) {
        geometries.forEach(this::insert);
    }

    public void insert(T geom) {
        stRtree.insert(geom.getTrajectory().getLineString().getEnvelopeInternal(), geom);
    }

    public List<T> query(Envelope envelope) {
        return stRtree.query(envelope);
    }

    public List<T> query(Geometry geometry) {
        return query(geometry.getEnvelopeInternal());
    }

    /**
     * STRTree use `==` to just the equality of objects int the tree,
     * so only support for removing object with the same address.
     */
    public void remove(T geom) {
        stRtree.remove(geom.getTrajectory().getLineString().getEnvelopeInternal(), geom);
    }

    public int size() {
        return stRtree.size();
    }

    @Override
    public String toString() {
        return stRtree.toString();
    }
}
