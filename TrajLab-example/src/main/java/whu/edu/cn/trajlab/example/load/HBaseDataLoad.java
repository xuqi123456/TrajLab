package whu.edu.cn.trajlab.example.load;

import com.fasterxml.jackson.core.JsonParseException;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.core.util.IOUtils;
import whu.edu.cn.trajlab.example.conf.ExampleConfig;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;

import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class HBaseDataLoad extends TestCase {
    private static final Logger LOGGER = Logger.getLogger(HBaseDataLoad.class);

    public void testLoadHBase() throws JsonParseException {
        String inPath = Objects.requireNonNull(
                HBaseDataLoad.class.getResource("/ioconf/HBaseLoadConfig.json")).getPath();
        String fileStr = IOUtils.readLocalTextFile(inPath);
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init loading from HBase Session...");
        boolean isLocal = true;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HBaseDataLoad.class.getName(), isLocal)) {
            ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
            JavaRDD<Trajectory> trajRDD =
                    iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig());
            LOGGER.info("Successfully load data from HBase");
            trajRDD.collect().forEach(System.out::println);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public static List<Trajectory> getLoadHBase() throws JsonParseException {
        String inPath = Objects.requireNonNull(
                HBaseDataLoad.class.getResource("/ioconf/HBaseLoadConfig.json")).getPath();
        String fileStr = IOUtils.readLocalTextFile(inPath);
        ExampleConfig exampleConfig = ExampleConfig.parse(fileStr);
        LOGGER.info("Init loading from HBase Session...");
        boolean isLocal = true;
        try (SparkSession sparkSession = SparkSessionUtils.createSession(exampleConfig.getLoadConfig(),
                HBaseDataLoad.class.getName(), isLocal)) {
            ILoader iLoader = ILoader.getLoader(exampleConfig.getLoadConfig());
            JavaRDD<Trajectory> trajRDD =
                    iLoader.loadTrajectory(sparkSession, exampleConfig.getLoadConfig());
            LOGGER.info("Successfully load data from HBase");
            return trajRDD.collect();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
