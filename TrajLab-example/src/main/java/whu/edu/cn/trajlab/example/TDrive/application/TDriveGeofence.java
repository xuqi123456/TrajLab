package whu.edu.cn.trajlab.example.TDrive.application;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import whu.edu.cn.trajlab.application.geofence.Geofence;
import whu.edu.cn.trajlab.application.geofence.GeofenceUtils;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.common.index.STRTreeIndex;
import whu.edu.cn.trajlab.core.common.index.TreeIndex;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.util.FSUtils;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static whu.edu.cn.trajlab.application.geofence.GeofenceUtils.readGeoFence;

/**
 * @author xuqi
 * @date 2024/04/15
 */
public class TDriveGeofence {
  public static void main(String[] args) throws IOException {
    String fs = args[0];
    String filePath = args[1];
    String fileStr = FSUtils.readFromFS(fs, filePath);

    String fencePath = args[2];
//    String outPath = args[4];

//        String inPath =
//
//     "D:\\bigdata\\TrajLab\\TrajLab-example\\src\\main\\java\\whu\\edu\\cn\\trajlab\\example\\TDrive\\dataprocess\\LoadConfig.json";
//        String fileStr = IOUtils.readLocalTextFile(inPath);
//        String fencePath = "D:\\毕业设计\\数据集\\beijing_landuse.csv";
//        String outPath = "D:\\毕业设计\\数据集\\fence\\trans.shp";
    // 2.解析配置文件
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    // 3.初始化sparkSession
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(TDriveGeofence.class.getName(), false)) {
      // 4.加载轨迹数据
      long loadStart = System.currentTimeMillis();
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(
              sparkSession, exampleConfig.getLoadConfig(), exampleConfig.getDataConfig());
      long loadEnd = System.currentTimeMillis();
      // 5.加载地理围栏数据并建立、广播索引
      long fenceStart = System.currentTimeMillis();
      List<Geometry> geometries = readGeoFence(fencePath);
      System.out.println("fence size = " + geometries.size());
//      List<Trajectory> trajectories = WKT2Traj.parseWKTPathToTrajectoryList(fencePath);
//      Traj2Shp.createShapefile(outPath, trajectories);
      STRTreeIndex<Geometry> treeIndex = GeofenceUtils.getIndexedGeoFence(fencePath);
      JavaSparkContext javaSparkContext =
          JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
      Broadcast<TreeIndex<Geometry>> treeIndexBroadcast = javaSparkContext.broadcast(treeIndex);
      // 6.空间交互关系计算
      Geofence<Geometry> geofenceFunc = new Geofence<>();
      JavaRDD<Tuple2<String, String>> res =
          trajRDD
              .map(traj -> geofenceFunc.geofence(traj, treeIndexBroadcast.getValue()))
              .filter(Objects::nonNull);
      // 7.收集结果
      List<Tuple2<String, String>> fencedTrajs = res.collect();
      System.out.println("Geofence Join Size = " + fencedTrajs.size());
      long fenceEnd = System.currentTimeMillis();
      System.out.println("Load cost " + (loadEnd - loadStart) + "ms");
      System.out.println("GeoFence cost " + (fenceEnd - fenceStart) + "ms");
    }
  }
}
