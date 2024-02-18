package whu.edu.cn.trajlab.query.query.advanced;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.GeoUtils;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.BufferQueryConditon;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.query.coprocessor.STCoprocessorQuery;
import whu.edu.cn.trajlab.query.coprocessor.autogenerated.QueryCondition;
import whu.edu.cn.trajlab.query.query.basic.AbstractQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * @author xuqi
 * @date 2024/01/29
 */
public class BufferQuery extends AbstractQuery {
  public BufferQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
    super(dataSet, abstractQueryCondition);
  }

  public BufferQuery(IndexTable targetIndexTable, AbstractQueryCondition abstractQueryCondition)
      throws IOException {
    super(targetIndexTable, abstractQueryCondition);
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
    setupTargetIndexTable();
    List<QueryCondition.Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
    BufferQueryConditon bufferQueryConditon = (BufferQueryConditon) abstractQueryCondition;
    Trajectory centralTrajectory = bufferQueryConditon.getCentralTrajectory();
    double disThreshold = bufferQueryConditon.getDisThreshold();
    Geometry buffer =
        centralTrajectory.getLineString().buffer(GeoUtils.getDegreeFromKm(disThreshold));
    String queryWindowWKT = bufferQueryConditon.getQueryWindowWKT(buffer);

    QueryCondition.QueryRequest bufferQueryRequest =
        QueryCondition.QueryRequest.newBuilder()
            .addAllRange(ranges)
            .setSt(
                QueryCondition.STQueryRequest.newBuilder()
                    .setSpatialQueryWindow(
                        QueryCondition.SpatialQueryWindow.newBuilder().setWkt(queryWindowWKT))
                    .setSpatialQueryType(QueryCondition.QueryType.INTERSECT)
                    .build())
            .build();
    return STCoprocessorQuery.executeQuery(targetIndexTable, bufferQueryRequest);
  }

  @Override
  public JavaRDD<Trajectory> getRDDQuery(SparkSession ss) throws IOException {
    List<RowKeyRange> indexRanges = getSplitRanges(abstractQueryCondition);
    JavaSparkContext context = SparkUtils.getJavaSparkContext(ss);
    JavaRDD<RowKeyRange> rowKeyRangeJavaRDD = context.parallelize(indexRanges);

    return rowKeyRangeJavaRDD
        .groupBy(RowKeyRange::getShardKey)
        .flatMap(
            iteratorPair -> {
              // 对每个分区中的元素进行转换操作
              List<RowKeyRange> result = new ArrayList<>();
              for (RowKeyRange rowKeyRange : iteratorPair._2) {
                result.add(rowKeyRange);
              }
              return executeQuery(result).iterator();
            });
  }

  public List<RowKeyRange> getSplitRanges(AbstractQueryCondition abQc1) throws IOException {
    setupTargetIndexTable();
    if (abQc1 instanceof BufferQueryConditon){
      BufferQueryConditon bufferQueryConditon = (BufferQueryConditon) abQc1;
      Trajectory centralTrajectory = bufferQueryConditon.getCentralTrajectory();
      double disThreshold = bufferQueryConditon.getDisThreshold();
      Geometry buffer =
              centralTrajectory.getLineString().buffer(GeoUtils.getDegreeFromKm(disThreshold));
      SpatialQueryCondition spatialQueryCondition =
              new SpatialQueryCondition(buffer, SpatialQueryCondition.SpatialQueryType.INTERSECT);

      return targetIndexTable.getIndexMeta().getIndexStrategy().getPartitionScanRanges(spatialQueryCondition);
    }else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public IndexMeta findBestIndex() {
    Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
    // case 1: 无时间约束，找XZ2索引，或XZ2T索引
    if (map.containsKey(IndexType.XZ2)) {
      return IndexMeta.getBestIndexMeta(map.get(IndexType.XZ2));
    }
    // case 2: 无空间表，找TXZ2索引
    else {
      if (map.containsKey(IndexType.TXZ2)) {
        return IndexMeta.getBestIndexMeta(map.get(IndexType.TXZ2));
      }
    }
    return null;
  }

  @Override
  public String getQueryInfo() {
    return abstractQueryCondition.getConditionInfo();
  }
}
