package whu.edu.cn.trajlab.example.TDrive.dataprocess;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.application.tracluster.segment.TimeSegment;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.operator.transform.sink.Traj2Shp;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.db.enums.TimePeriod;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.util.List;

/**
 * @author xuqi
 * @date 2024/04/12
 */
public class dataSegment {
  public static void main(String[] args) throws JsonParseException {
    String inPath =
        "D:\\bigdata\\TrajLab\\TrajLab-example\\src\\main\\java\\whu\\edu\\cn\\trajlab\\example\\TDrive\\dataprocess\\LoadConfig.json";
    String outpath = "D:\\bigdata\\TrajLab\\output\\trans.shp";
    String fileStr = IOUtils.readLocalTextFile(inPath);
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    boolean isLocal = true;
    long start = System.currentTimeMillis();
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(
            exampleConfig.getLoadConfig(), HBaseDataStore.class.getName(), isLocal)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
          iLoader.loadTrajectory(
              sparkSession, exampleConfig.getLoadConfig(), exampleConfig.getDataConfig());
      TimeSegment timeSegment = new TimeSegment(1, TimePeriod.HOUR);
      JavaRDD<Trajectory> segment = timeSegment.segment(trajRDD);
      List<Trajectory> collect = segment.collect();
//      for (Trajectory trajectory : collect) {
//        if (trajectory.getObjectID().equals("1")) {
//          System.out.println(
//              trajectory.getTrajectoryFeatures().getStartTime()
//                  + " "
//                  + trajectory.getTrajectoryFeatures().getEndTime());
//        }
//      }
      Traj2Shp.createShapefile(outpath, collect.get(0));
      System.out.println(collect.size());
      long end = System.currentTimeMillis();
      System.out.println("cost : " + (end - start) / 1000 + " s");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
