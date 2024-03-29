package whu.edu.cn.trajlab.example.transform;

import com.fasterxml.jackson.core.JsonParseException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.operator.store.IStore;
import whu.edu.cn.trajlab.core.operator.transform.ParquetTransform;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/11/27
 */
public class CSVToParquet {
    private static final Logger LOGGER = LoggerFactory.getLogger(CSVToParquet.class);
    @Test
    public void transCSVToParquet(){
        String sourcePath = "D:/bigdata/oge-computation-ogc/src/main/resources/geolife";
        String outPutPath = "D:/bigdata/oge-computation-ogc/src/main/resources/outfiles/parquet";
        try(SparkSession sparkSession = SparkSessionUtils.createSession("parquet", true)){
            ParquetTransform.parseCSVToParquet(sourcePath, false, sparkSession, outPutPath);
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void loadFromParquetPoint() throws JsonParseException {
        String inPath =
                Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/load/ParquetLoadPoint.json"))
                        .getPath();
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
            JavaRDD<Trajectory> featuresJavaRDD =
                    trajRDD.map(
                            trajectory -> {
                                trajectory.getTrajectoryFeatures();
                                return trajectory;
                            });
            System.out.println(featuresJavaRDD.take(1));
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void storeTrajToParquet() throws JsonParseException {
        String inPath =
                Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/store/ParquetStore.json"))
                        .getPath();
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
            JavaRDD<Trajectory> featuresJavaRDD =
                    trajRDD.map(
                            trajectory -> {
                                trajectory.getTrajectoryFeatures();
                                return trajectory;
                            });
            IStore iStore = IStore.getStore(exampleConfig.getStoreConfig());
            iStore.storeTrajectory(featuresJavaRDD, sparkSession);
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void loadFromParquetTraj() throws JsonParseException {
        String inPath =
                Objects.requireNonNull(TrajToGeojson.class.getResource("/ioconf/load/ParquetLoadTraj.json"))
                        .getPath();
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
            JavaRDD<Trajectory> featuresJavaRDD =
                    trajRDD.map(
                            trajectory -> {
                                trajectory.getTrajectoryFeatures();
                                return trajectory;
                            });
            System.out.println(featuresJavaRDD.take(1));
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
