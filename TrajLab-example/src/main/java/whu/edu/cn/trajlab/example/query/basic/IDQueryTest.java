package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.basic.IDQuery;

import java.io.IOException;
import java.util.List;

import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class IDQueryTest extends TestCase{
    public static String moid = "010";

    public void testIDQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        IDQueryCondition idQueryCondition = new IDQueryCondition(moid);
        IDQuery idQuery = new IDQuery(dataSet, idQueryCondition);
        List<Trajectory> trajectories = idQuery.executeQuery();
        for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        assertEquals(23, trajectories.size());
    }
    public void testIDRDDQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        IDQueryCondition idQueryCondition = new IDQueryCondition(moid );
        IDQuery idQuery = new IDQuery(dataSet, idQueryCondition);
        boolean isLocal = true;
        try (SparkSession sparkSession =
                     SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
            JavaRDD<Trajectory> rddQuery = idQuery.getRDDQuery(sparkSession);
            List<Trajectory> trajectories = rddQuery.collect();
            for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        assertEquals(23, trajectories.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public void testRowkeyQuery() throws IOException {
        Database instance = Database.getInstance();
        Table table = instance.getTable(DATASET_NAME	+ "-ID-default");
        ResultScanner scanner = table.getScanner(new Scan());
        for (Result result : scanner) {
            byte[] row = result.getRow();
            String s = Bytes.toString(row);
            System.out.println(s);
        }

    }
}
