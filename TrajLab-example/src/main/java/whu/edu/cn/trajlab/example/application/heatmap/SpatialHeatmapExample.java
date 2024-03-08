package whu.edu.cn.trajlab.example.application.heatmap;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Point;
import whu.edu.cn.trajlab.application.heatmap.GridRDD.TileGridDataRDD;
import whu.edu.cn.trajlab.application.heatmap.SaveAsImage;
import whu.edu.cn.trajlab.application.heatmap.SpatialHeatMap;
import whu.edu.cn.trajlab.application.heatmap.aggenum.TileAggregateType;
import whu.edu.cn.trajlab.application.heatmap.function.MapTrajectory;
import whu.edu.cn.trajlab.application.heatmap.tile.TileResult;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.transform.TrajToGeojson;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/03/06
 */
public class SpatialHeatmapExample {
  private static final Logger LOGGER = Logger.getLogger(SpatialHeatmapExample.class);

  public static void main(String[] args) throws Exception {
    String inPath =
        Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/LoadConfig.json"))
            .getPath();
    String outPath =
        "D:/bigdata/TrajLab/TrajLab-example/src/main/resources/outfiles/heatmap/";
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

      JavaRDD<Point> pointJavaRDD = MapTrajectory.FlatMapTrajectoryToPoints(trajRDD);

      TileGridDataRDD<Point, Double> tileGridDataRDD =
          new TileGridDataRDD<>(pointJavaRDD, TileAggregateType.SUM, 15);

      JavaRDD<TileResult<Double>> heatmapRDD = SpatialHeatMap.heatmap(tileGridDataRDD, 13);
//      List<TileResult<Double>> collect = heatmapRDD.collect();
//      System.out.println(collect);

      LOGGER.info("TrajLab Spatial HeatMap Example");
      SaveAsImage<Double> saveAsImage = new SaveAsImage<>(outPath);
      saveAsImage.saveHeatMapAsImage(heatmapRDD);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
