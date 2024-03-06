package whu.edu.cn.trajlab.example.application.segment;

import com.alibaba.fastjson.JSONObject;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import whu.edu.cn.trajlab.application.tracluster.segment.MdlSegment;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.operator.transform.sink.Traj2GeoJson;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.transform.TrajToGeojson;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/03/01
 */
public class MdlSegmentTest {
    private static final Logger LOGGER = Logger.getLogger(MdlSegmentTest.class);
    @Test
    public void TrajSegment() throws IOException {
        String inPath =
                Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/LoadConfig.json"))
                        .getPath();
    String outPath =
        "D:/bigdata/TrajLab/TrajLab-example/src/main/resources/outfiles/segment/trans.geojson";
        String outPath1 = "D:/bigdata/TrajLab/TrajLab-example/src/main/resources/outfiles/segment/transSeg.geojson";
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
            List<Trajectory> takeRDD = trajRDD.take(1);
            LOGGER.info("Finished!");
            JSONObject jsonObject = Traj2GeoJson.convertTrajListToGeoJson(takeRDD);
            IOUtils.writeStringToFile(outPath, jsonObject.toString());

            MdlSegment mdlSegment = new MdlSegment();
            JavaSparkContext javaSparkContext = SparkUtils.getJavaSparkContext(sparkSession);
            JavaRDD<Trajectory> segment = mdlSegment.segment(javaSparkContext.parallelize(takeRDD));
            List<Trajectory> collect = segment.collect();
            JSONObject jsonObject1 = Traj2GeoJson.convertTrajListToGeoJson(collect);
            IOUtils.writeStringToFile(outPath1, jsonObject1.toString());

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
