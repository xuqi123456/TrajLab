package whu.edu.cn.trajlab.example.TDrive.application;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.application.tracluster.dbscan.cluster.DBCluster;
import whu.edu.cn.trajlab.application.tracluster.dbscan.cluster.DBScanCluster;
import whu.edu.cn.trajlab.application.tracluster.dbscan.cluster.DBScanTraLine;
import whu.edu.cn.trajlab.application.tracluster.segment.MdlSegment;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.core.operator.transform.sink.Traj2Shp;
import whu.edu.cn.trajlab.example.TDrive.basicQuery.CreateQueryWindow;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.BasicQuery;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static whu.edu.cn.trajlab.query.query.QueryConf.*;

/**
 * @author xuqi
 * @date 2024/04/14
 */
public class TDriveCluster {
    public static void main(String[] args){

        String dataSetName = args[0];
        String spatialSize = args[1];
        String par = args[2];
        String path = args[3];
        String spatialQueryWindow = CreateQueryWindow.createSpatialQueryWindow(Double.parseDouble(spatialSize));
        long start;
        long end;

        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "XZ2");
        conf.set(DATASET_NAME, dataSetName);
        conf.set(SPATIAL_WINDOW, spatialQueryWindow);
        BasicQuery basicQuery1 = new BasicQuery(conf);
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), true, par)) {
            JavaRDD<Trajectory> rddScanQuery = basicQuery1.getRDDScanQuery(sparkSession);
            List<Trajectory> list = rddScanQuery.collect();

            start = System.currentTimeMillis();
            MdlSegment mdlSegment = new MdlSegment(5);
            JavaRDD<Trajectory> segment = mdlSegment.segment(rddScanQuery);
            List<Trajectory> collect = segment.collect();

            DBScanCluster dbScanCluster = new DBScanCluster(5, 5);
            List<DBCluster> dbClusters = dbScanCluster.doCluster(segment);

            for (DBCluster dbCluster : dbClusters) {
                HashSet<DBScanTraLine> trajSet = dbCluster.getTrajSet();
                ArrayList<Trajectory> trajectories = new ArrayList<>();
                for (DBScanTraLine dbScanTraLine : trajSet) {
                    trajectories.add(dbScanTraLine.getTrajectory());
                }
                String outpath = path + "cluster" + spatialSize + "_" + dbCluster.getClusterID() + ".shp";
                Traj2Shp.createShapefile(outpath, trajectories);
            }
            end = System.currentTimeMillis();
            long cost = (end - start);

            // 计算分钟和剩余秒数
            long minutes = cost / 60000;
            long seconds = (cost % 60000) / 1000;
            System.out.println("source data size = " + list.size());
            System.out.println("segement data size = " + collect.size());
            System.out.println("Distributed Data Size : " + dbClusters.size());
            // 使用 printf 格式化字符串打印时间
            System.out.printf("Distributed Query cost %d minutes %d seconds\n", minutes, seconds);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
