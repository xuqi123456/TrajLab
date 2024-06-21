package whu.edu.cn.trajlab.query.query.basic;

import whu.edu.cn.trajlab.core.conf.load.HBaseLoadConfig;
import whu.edu.cn.trajlab.core.operator.load.ILoader;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.meta.DataSetMeta;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.MetaTable;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/12/10
 */
public class DataSetQuery extends AbstractQuery{
  private static final Logger logger = LoggerFactory.getLogger(DataSetQuery.class);
  private static final Database instance;

  static {
    try {
      instance = Database.getInstance();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static final MetaTable metaTable;

  static {
    try {
      metaTable = instance.getMetaTable();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private String dataSetName;
  private HBaseLoadConfig loadConfig;

  public DataSetQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) throws IOException {
    super(dataSet, abstractQueryCondition);
    this.dataSetName = dataSet.getDataSetMeta().getDataSetName();
    this.loadConfig = new HBaseLoadConfig(dataSetName);
  }

  public MetaTable getMetaTable() {
    return metaTable;
  }

  public String getDataSetName() {
    return dataSetName;
  }

  public HBaseLoadConfig getLoadConfig() {
        return loadConfig;
    }

  public void setDataSetName(String dataSetName) {
    this.dataSetName = dataSetName;
  }

  public void setLoadConfig(HBaseLoadConfig loadConfig) {
    this.loadConfig = loadConfig;
  }

  public DataSetMeta getDataSetMeta() throws IOException {
    return metaTable.getDataSetMeta(dataSetName);
  }

  public JavaRDD<Trajectory> loadDataSet(SparkSession ss) throws IOException {
    ILoader loader = ILoader.getLoader(loadConfig);
    return loader.loadTrajectory(ss, loadConfig);
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) {
    return null;
  }

  @Override
  public List<Trajectory> getFinalFilter(List<Trajectory> list) {
    return list;
  }

  @Override
  public JavaRDD<Trajectory> getRDDQuery(SparkSession sc) throws IOException {
    return loadDataSet(sc);
  }

  @Override
  public IndexMeta findBestIndex() {
    return null;
  }

  @Override
  public String getQueryInfo() {
    return abstractQueryCondition.getConditionInfo();
  }
}
