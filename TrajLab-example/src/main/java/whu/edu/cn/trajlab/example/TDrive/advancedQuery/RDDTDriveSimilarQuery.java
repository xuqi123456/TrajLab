package whu.edu.cn.trajlab.example.TDrive.advancedQuery;

import static whu.edu.cn.trajlab.query.query.QueryConf.*;
import static whu.edu.cn.trajlab.query.query.QueryConf.K;

import java.io.IOException;
import java.util.Base64;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.core.operator.transform.sink.Traj2Shp;
import whu.edu.cn.trajlab.example.TDrive.basicQuery.TDriveIDQuery;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.AdvancedQuery;

/**
 * @author xuqi
 * @date 2024/04/13
 */
public class RDDTDriveSimilarQuery {
    public static void main(String[] args) throws IOException {
        String idDataSetName = args[0];
        String dataSetName = args[1];
        String dis = args[2];
        String par = args[3];
        String outpath = args[4];
        long start;
        long end;
        List<Trajectory> collect;
        Trajectory coreTrajectory = TDriveIDQuery.getCoreTrajectory(idDataSetName, "1");
        byte[] byteArray = SerializerUtils.serializeObject(coreTrajectory);
        // 将字节数组转换为Base64编码的字符串
        String trajectory_str = Base64.getEncoder().encodeToString(byteArray);
        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "SIMILAR");
        conf.set(DATASET_NAME, dataSetName);
        conf.set(CENTER_TRAJECTORY, trajectory_str);
        conf.set(START_TIME, "2008-02-02 00:00:00");
        conf.set(END_TIME, "2008-02-03 00:00:00");
        conf.set(DIS_THRESHOLD, dis);
        AdvancedQuery advancedQuery = new AdvancedQuery(conf);
        long startAll = System.currentTimeMillis();
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(RDDTDriveSimilarQuery.class.getName(), true, par)) {
            start = System.currentTimeMillis();
            JavaRDD<Trajectory> rddScanQuery = advancedQuery.getRDDScanQuery(sparkSession);
            collect = rddScanQuery.collect();
            end = System.currentTimeMillis();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        long endAll = System.currentTimeMillis();
        long cost = (end - start);
        long costAll = (endAll - startAll);

        // 计算分钟和剩余秒数
        long minutes = cost / 60000;
        long seconds = (cost % 60000) / 1000;

        // 计算分钟和剩余秒数
        long minutesAll = costAll / 60000;
        long secondsAll = (costAll % 60000) / 1000;
        Traj2Shp.createShapefile(outpath, collect);
        System.out.println("Distributed Data Size : " + collect.size());
        // 使用 printf 格式化字符串打印时间
        System.out.printf("Distributed Query cost %d minutes %d seconds\n", minutes, seconds);
        System.out.printf("ToTal Distributed Query cost %d minutes %d seconds\n", minutesAll, secondsAll);
    }
}
