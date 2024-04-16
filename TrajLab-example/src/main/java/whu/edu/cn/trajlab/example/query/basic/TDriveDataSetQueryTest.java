package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.DataSetQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.meta.DataSetMeta;
import whu.edu.cn.trajlab.example.store.HBaseDataStore;
import whu.edu.cn.trajlab.example.util.SparkSessionUtils;
import whu.edu.cn.trajlab.query.query.basic.DataSetQuery;

import java.io.IOException;
import java.util.List;

import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

/**
 * @author xuqi
 * @date 2024/01/24
 */
public class TDriveDataSetQueryTest extends TestCase {

  public void testDataSetQuery() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    DataSetQueryCondition dataSetQueryCondition = new DataSetQueryCondition(DATASET_NAME);
    DataSetQuery dataSetQuery = new DataSetQuery(dataSet, dataSetQueryCondition);
    boolean isLocal = true;
    try (SparkSession sparkSession =
        SparkSessionUtils.createSession(HBaseDataStore.class.getName(), isLocal)) {
      JavaRDD<Trajectory> rddQuery = dataSetQuery.getRDDQuery(sparkSession);
      List<Trajectory> trajectories = rddQuery.collect();
      System.out.println(trajectories.size());
      for (Trajectory trajectory : trajectories) {
        System.out.println(trajectory);
      }
      assertEquals(23, trajectories.size());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  public void testGetMeta() throws IOException {
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(DATASET_NAME);
    DataSetQueryCondition dataSetQueryCondition = new DataSetQueryCondition(DATASET_NAME);
    DataSetQuery dataSetQuery = new DataSetQuery(dataSet, dataSetQueryCondition);
    DataSetMeta dataSetMeta = dataSetQuery.getDataSetMeta();
    System.out.println(dataSetMeta.toString());
  }
}
