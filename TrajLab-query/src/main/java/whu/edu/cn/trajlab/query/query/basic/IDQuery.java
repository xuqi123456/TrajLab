package whu.edu.cn.trajlab.query.query.basic;

import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import org.apache.spark.api.java.JavaRDD;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/12/11
 */
public class IDQuery extends AbstractQuery {

  public IDQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
    super(dataSet, abstractQueryCondition);
  }

  public IDQuery(IndexTable indexTable, AbstractQueryCondition abstractQueryCondition)
      throws IOException {
    super(indexTable, abstractQueryCondition);
  }

  @Override
  public List<RowKeyRange> getIndexRanges() throws IOException {
    setupTargetIndexTable();
    return targetIndexTable.getIndexMeta().getIndexStrategy().getScanRanges(abstractQueryCondition);
  }

  @Override
  public List<Trajectory> executeQuery() throws IOException {
    List<RowKeyRange> rowKeyRanges = getIndexRanges();
    return executeQuery(rowKeyRanges);
  }

  @Override
  public JavaRDD<Trajectory> query() throws IOException {
    return null;
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
    return null;
  }

  @Override
  public IndexMeta findBestIndex() {
    Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
    // find a ID index
    List<IndexMeta> indexList = null;
    if (map.containsKey(IndexType.ID)) {
      indexList = map.get(IndexType.ID);
    }
    if (indexList != null) {
      return IndexMeta.getBestIndexMeta(indexList);
    }
    // no spatial index so we will do a full table scan, we select a main index.
    return dataSet.getDataSetMeta().getCoreIndexMeta();
  }

  @Override
  public String getQueryInfo() {
    return abstractQueryCondition.getConditionInfo();
  }
}
