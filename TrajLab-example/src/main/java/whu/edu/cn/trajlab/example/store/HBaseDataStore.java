package whu.edu.cn.trajlab.example.store;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.operator.store.IStore;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.io.IOException;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2023/12/05
 */
public class HBaseDataStore extends TestCase {
    private static final Logger LOGGER = LoggerFactory.getLogger(HBaseDataStore.class);
    public void testStoreHBase() throws IOException {
        String inPath =
                Objects.requireNonNull(HBaseDataStore.class.getResource("/ioconf/HBaseStoreConfig.json"))
                        .getPath();
        String fileStr = IOUtils.readLocalTextFile(inPath);
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init sparkSession...");
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(
                             exampleConfig.getLoadConfig(), HBaseDataStore.class.getName(), isLocal)) {
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
            IStore iStore =
                    IStore.getStore(exampleConfig.getStoreConfig());
            iStore.storeTrajectory(featuresJavaRDD);
            LOGGER.info("Finished!");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void testDeleteDataSet() throws IOException {
        Database instance = Database.getInstance();
        instance.deleteDataSet("TRAJECTORY_TEST");
    }
}
