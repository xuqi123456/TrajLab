package whu.edu.cn.trajlab.core.operator.store;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.NotImplementedError;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.conf.store.*;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public interface IStore extends Serializable {
    void storeTrajectory(JavaRDD<Trajectory> t) throws Exception;
    void storeTrajectory(JavaRDD<Trajectory> t, SparkSession ss) throws Exception;

    static IStore getStore(IStoreConfig storeConfig) {
        switch (storeConfig.getStoreType()) {
            case HDFS:
                if (storeConfig instanceof HDFSStoreConfig) {
                    return new HDFSStore((HDFSStoreConfig) storeConfig);
                }
            case STANDALONE:
                if (storeConfig instanceof StandaloneStoreConfig) {
                    return new StandaloneStore((StandaloneStoreConfig) storeConfig);
                }
            case HBASE:
                if (storeConfig instanceof HBaseStoreConfig) {
                    Configuration conf = HBaseConfiguration.create();
                    return new HBaseStore((HBaseStoreConfig) storeConfig, conf);
                }
            case HIVE:
                if (storeConfig instanceof HiveStoreConfig) {
                    return new HiveStore((HiveStoreConfig) storeConfig);
                }
                throw new NoSuchMethodError();
            default:
                throw new NotImplementedError();
        }
    }
}
