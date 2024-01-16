package whu.edu.cn.trajlab.core.operator.store;

import whu.edu.cn.trajlab.core.conf.store.HiveStoreConfig;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HiveStore implements IStore{
    //TODO hive store
    private final HiveStoreConfig storeConfig;

    public HiveStore(HiveStoreConfig storeConfig) {
        this.storeConfig = storeConfig;
    }

    @Override
    public void storeTrajectory(JavaRDD<Trajectory> t) throws Exception {

    }

    @Override
    public void storeTrajectory(JavaRDD<Trajectory> t, SparkSession ss) throws Exception {

    }
}
