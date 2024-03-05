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
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;


/**
 * @author xuqi
 * @date 2024/03/02
 */
public class DBScanCluster implements Serializable {
  private final int minLines;
  protected double radius;//km

  public DBScanCluster(int minLines, double radius) {
    this.minLines = minLines;
    this.radius = radius;
  }

  public MinimumBoundingBox getTraEnv(JavaRDD<Trajectory> traRDD) {
    // 创建AtomicReference对象，初始值为全局box对象
    AtomicReference<MinimumBoundingBox> boxRef = new AtomicReference<>(null);

    // 对RDD进行foreachPartition操作
    traRDD.foreachPartition(
        partitionItr -> {
          // 创建一个临时的局部box对象
          MinimumBoundingBox localBox = null;

          while (partitionItr.hasNext()) {
            Trajectory t = partitionItr.next();
            if (localBox == null) {
              localBox = t.getTrajectoryFeatures().getMbr();
            } else {
              localBox = localBox.union(t.getTrajectoryFeatures().getMbr());
            }
          }
          // 将局部box对象合并到全局box对象中
          MinimumBoundingBox finalLocalBox = localBox;

          boxRef.getAndUpdate(
              currentBox -> {
                if (currentBox == null) return finalLocalBox;
                else {
                  return currentBox.union(finalLocalBox);
                }
              });
        });
    return boxRef.get();
  }

  public List<DBCluster> doCluster(SparkSession ss, JavaRDD<Trajectory> traRDD) {
    JavaSparkContext context = SparkUtils.getJavaSparkContext(ss);
    MinimumBoundingBox traEnv = getTraEnv(traRDD);
    DBPartition dbPartition = new DBPartition(radius, traEnv);

    // 将轨迹映射到网格
    JavaRDD<Tuple2<Grid, Trajectory>> gridRDD =
        traRDD.flatMap(
            t -> {
              List<Grid> grids = dbPartition.calTrajectoryGridSet(t);
              ArrayList<Tuple2<Grid, Trajectory>> list = new ArrayList<>();
              for (Grid grid : grids) {
                list.add(new Tuple2<>(grid, t));
              }
              return list.iterator();
            });
//    // 构建网格映射字典
//    ConcurrentHashMap<Grid, List<Trajectory>> concurrentTraHashMap = new ConcurrentHashMap<>();
    JavaPairRDD<Grid, Iterable<Tuple2<Grid, Trajectory>>> gridIterableJavaPairRDD =
        gridRDD.groupBy(Tuple2::_1);
//    gridIterableJavaPairRDD.foreach(
//        partition -> {
//          Grid key = partition._1;
//          Iterable<Tuple2<Grid, Trajectory>> values = partition._2;
//
//          List<Trajectory> trajectoryList = new ArrayList<>();
//          values.forEach(t -> trajectoryList.add(t._2));
//
//          concurrentTraHashMap.put(key, trajectoryList);
//        });
//    Broadcast<ConcurrentHashMap<Grid, List<Trajectory>>> mapBroadcast =
//        context.broadcast(concurrentTraHashMap);
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
      if (dfdResult.size() < minLines) traLine.setNoise(true);
      else {
        DBCluster dbCluster = new DBCluster();
        DBCluster expandCluster = expandCluster(traLine, dbCluster, dfdResult, rTreeIndex);
        clusters.add(expandCluster);
      }
    }
    return clusters;
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
    for (DBScanTraLine scanTraLine : dfdResult) {
      if (!scanTraLine.isVisited()) {
        scanTraLine.setVisited(true);
        Envelope envelopeInternal =
            scanTraLine.getTrajectory().getLineString().getEnvelopeInternal();
        envelopeInternal.expandBy(GeoUtils.getDegreeFromKm(radius));
        List<DBScanTraLine> query = rTreeIndex.query(envelopeInternal);
        List<DBScanTraLine> dfdList = DFDDistance(query, scanTraLine.getTrajectory());
        if (dfdList.size() >= minLines) {
          dfdResult.addAll(dfdList);
          dbCluster.addDBScanTraLine(scanTraLine);
        } else scanTraLine.setNoise(true);
      }
    }
    dbCluster.setTrajSet(dfdResult);
    return dbCluster;
  }
}
