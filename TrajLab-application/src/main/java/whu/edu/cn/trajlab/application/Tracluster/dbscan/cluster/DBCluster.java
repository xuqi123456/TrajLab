package whu.edu.cn.trajlab.application.Tracluster.dbscan.cluster;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.GeoUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/02
 */
public class DBCluster implements Serializable {
    private Envelope clusterEnv;
    private Point center;
    private HashSet<DBScanTraLine> trajCoreSet;
    private HashSet<DBScanTraLine> trajSet;

    public DBCluster() {
    }


    public DBCluster(Envelope clusterEnv, Point center, HashSet<DBScanTraLine> trajCoreSet, HashSet<DBScanTraLine> trajSet) {
        this.clusterEnv = clusterEnv;
        this.center = center;
        this.trajCoreSet = trajCoreSet;
        this.trajSet = trajSet;
    }

    public boolean checkSameCoreTra(DBCluster dbCluster){
        HashSet<DBScanTraLine> trajCoreSet1 = dbCluster.getTrajCoreSet();
        HashSet<DBScanTraLine> hashSet = new HashSet<>(trajCoreSet1);
        hashSet.retainAll(trajCoreSet);
        return !hashSet.isEmpty();
    }
    public boolean checkDistance(DBCluster dbCluster, double distance){
        return GeoUtils.getEuclideanDistanceKM(center, dbCluster.getCenter()) <= distance;
    }
    public static DBCluster unionDBCluster(DBCluster cluster1, DBCluster cluster2) {
        HashSet<DBScanTraLine> trajCoreSet1 = cluster1.getTrajCoreSet();
        HashSet<DBScanTraLine> trajCoreSet2 = cluster2.getTrajCoreSet();
        trajCoreSet1.addAll(trajCoreSet2);
        HashSet<DBScanTraLine> trajSet1 = cluster1.getTrajSet();
        HashSet<DBScanTraLine> trajSet2 = cluster2.getTrajSet();
        trajSet1.addAll(trajSet2);
        Envelope clusterEnv1 = cluster1.getClusterEnv();
        clusterEnv1.expandToInclude(cluster2.getClusterEnv());
        Geometry envelopeGeometry = GeoUtils.createEnvelopeGeometry(clusterEnv1);
        Point centroid = envelopeGeometry.getCentroid();
        return new DBCluster(clusterEnv1, centroid, trajCoreSet1, trajSet1);
    }


    public Envelope getClusterEnv() {
        return clusterEnv;
    }

    public Point getCenter() {
        return center;
    }

    public HashSet<DBScanTraLine> getTrajSet() {
        return trajSet;
    }

    public HashSet<DBScanTraLine> getTrajCoreSet() {
        return trajCoreSet;
    }


    public void setTrajSet(List<DBScanTraLine> trajSet) {
        HashSet<DBScanTraLine> traLines = new HashSet<>();
        traLines.addAll(trajSet);
        this.trajSet = traLines;
    }

    public void addDBScanTraLine(DBScanTraLine traLine){
        Envelope envelopeInternal = traLine.getTrajectory().getLineString().getEnvelopeInternal();
        if(clusterEnv == null){
            clusterEnv = envelopeInternal;
        }else clusterEnv.expandToInclude(envelopeInternal);
        Geometry envelopeGeometry = GeoUtils.createEnvelopeGeometry(clusterEnv);
        center = envelopeGeometry.getCentroid();
        trajCoreSet.add(traLine);
    }
}
