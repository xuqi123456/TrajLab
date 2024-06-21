package whu.edu.cn.trajlab.example.TDrive.application;

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
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.io.IOException;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/04/15
 */
public class TDriveHeatmap {
  public static void main(String[] args) throws IOException {

    String fs = args[0];
    String filePath = args[1];
    String fileStr = FSUtils.readFromFS(fs, filePath);
//    String fileStr = IOUtils.readLocalTextFile(filePath);
    String outPath = args[2];
    String par = args[3];

    // 本地测试时可以传入第三个参数，指定是否本地master运行
    boolean isLocal = false;

    // 2.解析配置文件
    ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(TDriveHeatmap.class.getName(), isLocal, par)) {
      ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
      JavaRDD<Trajectory> trajRDD =
              iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig());

      JavaRDD<Point> pointJavaRDD = MapTrajectory.FlatMapTrajectoryToPoints(trajRDD);

      TileGridDataRDD<Point, Double> tileGridDataRDD =
          new TileGridDataRDD<>(pointJavaRDD, TileAggregateType.SUM, 12);

      JavaRDD<TileResult<Double>> heatmapRDD = SpatialHeatMap.heatmap(tileGridDataRDD, 9);

      SaveAsImage<Double> saveAsImage = new SaveAsImage<>(outPath);
      saveAsImage.saveHeatMapAsImage(heatmapRDD);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
