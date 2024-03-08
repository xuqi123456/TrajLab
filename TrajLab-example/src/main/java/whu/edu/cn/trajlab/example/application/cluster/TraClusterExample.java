package whu.edu.cn.trajlab.example.application.cluster;

import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.core.JsonParseException;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.application.tracluster.dbscan.cluster.DBCluster;
import whu.edu.cn.trajlab.application.tracluster.dbscan.cluster.DBScanCluster;
import whu.edu.cn.trajlab.application.tracluster.dbscan.cluster.DBScanTraLine;
import whu.edu.cn.trajlab.application.tracluster.segment.MdlSegment;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.operator.transform.sink.Traj2GeoJson;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.application.segment.MdlSegmentTest;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.transform.TrajToGeojson;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/03/06
 */
public class TraClusterExample {
  private static final Logger LOGGER = Logger.getLogger(TraClusterExample.class);

  public static void main(String[] args) throws JsonParseException {
    String inPath =
        Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/LoadConfig.json"))
            .getPath();
    String outPath1 =
        "D:/bigdata/TrajLab/TrajLab-example/src/main/resources/outfiles/cluster/trans.geojson";
    String outPath = "D:/bigdata/TrajLab/TrajLab-example/src/main/resources/outfiles/cluster/";
    String fileStr = IOUtils.readLocalTextFile(inPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    LOGGER.info("Init sparkSession...");
    boolean isLocal = true;
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(
            exampleConfig.getLoadConfig(), TrajToGeojson.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(
              sparkSession, exampleConfig.getLoadConfig(), exampleConfig.getDataConfig());
      //            JavaRDD<Trajectory> featuresJavaRDD =
      //                    trajRDD.map(
      //                            trajectory -> {
      //                                trajectory.getTrajectoryFeatures();
      //                                return trajectory;
      //                            });
      List<Trajectory> takeRDD = trajRDD.collect();

      MdlSegment mdlSegment = new MdlSegment(0.5);
      JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext(sparkSession);
      JavaRDD<Trajectory> segment = mdlSegment.segment(javaSparkContext.parallelize(takeRDD));

      JSONObject jsonObjectS = Traj2GeoJson.convertTrajListToGeoJson(segment.collect());
      IOUtils.writeStringToFile(outPath1, jsonObjectS.toString());

      DBScanCluster dbScanCluster = new DBScanCluster(3, 5);
      List<DBCluster> dbClusters = dbScanCluster.doCluster(sparkSession, segment);

      for (DBCluster dbCluster : dbClusters) {
        HashSet<DBScanTraLine> trajSet = dbCluster.getTrajSet();
        ArrayList<Trajectory> trajectories = new ArrayList<>();
        for (DBScanTraLine dbScanTraLine : trajSet) {
          trajectories.add(dbScanTraLine.getTrajectory());
        }
        JSONObject jsonObject = Traj2GeoJson.convertTrajListToGeoJson(trajectories);
        IOUtils.writeStringToFile(
            outPath + dbCluster.getClusterID() + ".geojson", jsonObject.toString());
      }

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
