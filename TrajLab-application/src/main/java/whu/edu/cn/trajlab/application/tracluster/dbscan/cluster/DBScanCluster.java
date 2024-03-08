package whu.edu.cn.trajlab.application.tracluster.dbscan.cluster;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Envelope;
import scala.Tuple2;
import whu.edu.cn.trajlab.application.tracluster.dbscan.partition.DBPartition;
import whu.edu.cn.trajlab.application.tracluster.dbscan.partition.Grid;
import whu.edu.cn.trajlab.base.mbr.MinimumBoundingBox;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.DiscreteFrechetDistance;
import whu.edu.cn.trajlab.base.util.GeoUtils;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.application.tracluster.dbscan.index.RTreeIndex;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author xuqi
 * @date 2024/03/02
 */
class ListNode<V> {
  V val;
  ListNode<V> next;

  public ListNode(V val) {
    this.val = val;
  }
}
public class DBScanCluster implements Serializable {
  private final int minLines;
  protected double radius;//km

  public DBScanCluster(int minLines, double radius) {
    this.minLines = minLines;
    this.radius = radius;
  }

  public MinimumBoundingBox getTraEnv(JavaRDD<Trajectory> traRDD) {

    JavaRDD<MinimumBoundingBox> result = traRDD.mapPartitions(partitionItr -> {
      List<MinimumBoundingBox> localBoxes = new ArrayList<>();
      if (partitionItr.hasNext()) {
        MinimumBoundingBox localBox = null;
        while (partitionItr.hasNext()) {
          Trajectory t = partitionItr.next();
          if (localBox == null) {
            localBox = t.getTrajectoryFeatures().getMbr();
          } else {
            localBox = localBox.union(t.getTrajectoryFeatures().getMbr());
          }
        }
        localBoxes.add(localBox);
      }
      return localBoxes.iterator();
    });

    MinimumBoundingBox finalBox = result.reduce((box1, box2) -> box1.union(box2));

    return finalBox;
  }

  public List<DBCluster> doCluster(SparkSession ss, JavaRDD<Trajectory> traRDD) {
    MinimumBoundingBox traEnv = getTraEnv(traRDD);
    DBPartition dbPartition = new DBPartition(radius, traEnv);
    // 将轨迹映射到网格
    JavaRDD<Tuple2<Grid, Trajectory>> gridRDD =
        traRDD.flatMap(
            t -> {
              List<Grid> grids = dbPartition.calTrajectoryGridSet(t, traEnv);
              ArrayList<Tuple2<Grid, Trajectory>> list = new ArrayList<>();
              for (Grid grid : grids) {
                list.add(new Tuple2<>(grid, t));
              }
              return list.iterator();
            });
    JavaPairRDD<Grid, Iterable<Tuple2<Grid, Trajectory>>> gridIterableJavaPairRDD =
        gridRDD.groupBy(Tuple2::_1);

      // 构建分区R树
    JavaRDD<Tuple2<RTreeIndex<DBScanTraLine>, HashSet<DBScanTraLine>>> STRGridRDD =
        gridIterableJavaPairRDD.mapPartitions(
            t -> {
              HashSet<DBScanTraLine> resultSet = new HashSet<>();
              RTreeIndex<DBScanTraLine> treeIndex = new RTreeIndex<>();
              while (t.hasNext()) {
                Tuple2<Grid, Iterable<Tuple2<Grid, Trajectory>>> next = t.next();
                for (Tuple2<Grid, Trajectory> gridTrajectoryTuple : next._2) {
                  DBScanTraLine line = new DBScanTraLine(gridTrajectoryTuple._2);
                  treeIndex.insert(line);
                  resultSet.add(line);
                }
              }

              // 将完整的 R 树和所有数据作为结果返回
              List<Tuple2<RTreeIndex<DBScanTraLine>, HashSet<DBScanTraLine>>> result =
                  new ArrayList<>();
              result.add(new Tuple2<>(treeIndex, resultSet));
              return result.iterator();
            });
    // 进行分区dbscan聚类
    JavaRDD<DBCluster> dbClusterRDD = STRGridRDD.flatMap(str -> dbScanCluster(str).iterator());
    // 进行全局Cluster合并
    List<DBCluster> collectClusters = dbClusterRDD.collect();
    HashSet<DBCluster> globalClusters = new HashSet<>();
    int clusterID = 1;
    for (DBCluster collectCluster : collectClusters) {
      boolean merged = false;
      for (DBCluster dbCluster : globalClusters) {
        if (collectCluster.checkSameCoreTra(dbCluster) || collectCluster.checkDistance(dbCluster, radius) ) {
          merged = true;
          globalClusters.add(DBCluster.unionDBCluster(collectCluster, dbCluster));
          globalClusters.remove(dbCluster);
          break;
        }
      }
      if (!merged) {
        collectCluster.setClusterID(clusterID++);
        globalClusters.add(collectCluster); // 如果没有合并，则加入全局集合中
      }
    }
    return new ArrayList<>(globalClusters);
  }

  public List<DBCluster> dbScanCluster(
      Tuple2<RTreeIndex<DBScanTraLine>, HashSet<DBScanTraLine>> grid) {
    RTreeIndex<DBScanTraLine> rTreeIndex = grid._1;
    ArrayList<DBCluster> clusters = new ArrayList<>();

    for (DBScanTraLine traLine : grid._2) {
      if (traLine.isVisited()) continue;
      traLine.setVisited(true);
      Envelope envelopeInternal = traLine.getTrajectory().getLineString().getEnvelopeInternal();
      envelopeInternal.expandBy(GeoUtils.getDegreeFromKm(radius));
      List<DBScanTraLine> queryResult = rTreeIndex.query(envelopeInternal);
      List<DBScanTraLine> dfdResult = DFDDistance(queryResult, traLine.getTrajectory());
      if (checkMinLines(dfdResult) < minLines) traLine.setNoise(true);
      else {
        DBCluster dbCluster = new DBCluster();
        DBCluster expandCluster = expandCluster(traLine, dbCluster, dfdResult, rTreeIndex);
        clusters.add(expandCluster);
      }
    }
    return clusters;
  }
  public int checkMinLines(List<DBScanTraLine> dbScanTraLines){
    HashSet<String> hashSet = new HashSet<>();
    for (DBScanTraLine scanTraLine : dbScanTraLines) {
      hashSet.add(scanTraLine.getTrajectory().getObjectID());
    }
    return hashSet.size();
  }

  public List<DBScanTraLine> DFDDistance(List<DBScanTraLine> trajectories, Trajectory center) {
    ArrayList<DBScanTraLine> list = new ArrayList<>();
    for (DBScanTraLine trajectory : trajectories) {
      double dfd =
          DiscreteFrechetDistance.calculateDFD(
              trajectory.getTrajectory().getLineString(), center.getLineString());
      if (GeoUtils.getDegreeFromKm(radius) >= dfd) list.add(trajectory);
    }
    return list;
  }

  public DBCluster expandCluster(
      DBScanTraLine traLine,
      DBCluster dbCluster,
      List<DBScanTraLine> dfdResult,
      RTreeIndex<DBScanTraLine> rTreeIndex) {

    dbCluster.addDBScanTraLine(traLine);
    HashSet<DBScanTraLine> set = new HashSet<>(dfdResult);
    ListNode<DBScanTraLine> head = new ListNode<>(null);
    ListNode<DBScanTraLine> tail = head;
    for (DBScanTraLine line : dfdResult) {
      tail.next = new ListNode<>(line);
      tail = tail.next;
    }
    ListNode<DBScanTraLine> res = head.next;
    while (res != null){
      DBScanTraLine scanTraLine = res.val;
      if (!scanTraLine.isVisited()) {
        scanTraLine.setVisited(true);
        Envelope envelopeInternal =
                scanTraLine.getTrajectory().getLineString().getEnvelopeInternal();
        envelopeInternal.expandBy(GeoUtils.getDegreeFromKm(radius));
        List<DBScanTraLine> query = rTreeIndex.query(envelopeInternal);
        List<DBScanTraLine> dfdList = DFDDistance(query, scanTraLine.getTrajectory());
        if (checkMinLines(dfdResult) >= minLines) {
          for (DBScanTraLine line : dfdList) {
            if(!set.contains(line)){
              set.add(line);
              tail.next = new ListNode<>(line);
              tail = tail.next;
            }
          }
          dbCluster.addDBScanTraLine(scanTraLine);
        } else scanTraLine.setNoise(true);
      }
      res = res.next;
    }

    dbCluster.setTrajSet(set);
    return dbCluster;
  }
}
