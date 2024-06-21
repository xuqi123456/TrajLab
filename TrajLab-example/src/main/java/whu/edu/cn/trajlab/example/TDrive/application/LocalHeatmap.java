package whu.edu.cn.trajlab.example.TDrive.application;

import java.io.IOException;
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
import whu.edu.cn.trajlab.core.util.FSUtils;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

/**
 * @author xuqi
 * @date 2024/04/15
 */
public class LocalHeatmap {
  public static void main(String[] args) throws IOException {

    String inPath =
            "D:\\bigdata\\TrajLab\\TrajLab-example\\src\\main\\java\\whu\\edu\\cn\\trajlab\\example\\TDrive\\dataprocess\\LoadConfig.json";
    String outPath = "D:\\毕业设计\\数据集\\map";
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

      JavaRDD<Point> pointJavaRDD = MapTrajectory.FlatMapTrajectoryToPoints(trajRDD);

      TileGridDataRDD<Point, Double> tileGridDataRDD =
          new TileGridDataRDD<>(pointJavaRDD, TileAggregateType.SUM, 15);

      JavaRDD<TileResult<Double>> heatmapRDD = SpatialHeatMap.heatmap(tileGridDataRDD, 12);

      SaveAsImage<Double> saveAsImage = new SaveAsImage<>(outPath);
      saveAsImage.saveHeatMapAsImage(heatmapRDD);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
