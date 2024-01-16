package whu.edu.cn.trajlab.core.operator.load;

import whu.edu.cn.trajlab.core.conf.data.IDataConfig;
import whu.edu.cn.trajlab.core.conf.load.ILoadConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HiveLoader implements ILoader{
    //TODO hive loader
    @Override
    public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig, IDataConfig dataConfig) {
        return null;
    }

    @Override
    public JavaRDD<Trajectory> loadTrajectory(SparkSession ss, ILoadConfig loadConfig) throws IOException {
        return null;
    }
}
