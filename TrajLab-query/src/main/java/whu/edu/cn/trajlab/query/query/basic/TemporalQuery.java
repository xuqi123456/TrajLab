package whu.edu.cn.trajlab.query.query.basic;

import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajlab.db.condition.IDTemporalQueryCondition;
import whu.edu.cn.trajlab.query.coprocessor.STCoprocessorQuery;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.query.coprocessor.autogenerated.QueryCondition;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/12/11
 */
public class TemporalQuery extends AbstractQuery implements Serializable {

  public TemporalQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
    super(dataSet, abstractQueryCondition);
  }

  public TemporalQuery(IndexTable indexTable, AbstractQueryCondition abstractQueryCondition)
      throws IOException {
    super(indexTable, abstractQueryCondition);
  }

  @Override
  public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
    setupTargetIndexTable();
    List<QueryCondition.Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
    TemporalQueryCondition temporalQueryCondition = (TemporalQueryCondition) abstractQueryCondition;
    List<QueryCondition.TemporalQueryWindow> temporalQueryWindows =
        buildProtoTemporalWindows(temporalQueryCondition);

    QueryCondition.QueryRequest timeQueryRequest =
        QueryCondition.QueryRequest.newBuilder()
            .addAllRange(ranges)
            .setSt(
                QueryCondition.STQueryRequest.newBuilder()
                    .setTemporalQueryType(
                        temporalQueryCondition.getTemporalQueryType() == TemporalQueryType.CONTAIN
                            ? QueryCondition.QueryType.CONTAIN
                            : QueryCondition.QueryType.INTERSECT)
                    .setTemporalQueryWindows(
                        QueryCondition.TemporalQueryWindows.newBuilder()
                            .addAllTemporalQueryWindow(temporalQueryWindows)
                            .build())
                    .build())
            .setQueryOperation(QueryCondition.QueryMethod.ST)
            .build();

    return STCoprocessorQuery.executeQuery(targetIndexTable, timeQueryRequest);
  }

  @Override
  public List<Trajectory> getFinalFilter(List<Trajectory> list) throws ParseException {
    TemporalQueryCondition temporalQueryCondition = null;
    if(abstractQueryCondition instanceof TemporalQueryCondition){
      temporalQueryCondition = (TemporalQueryCondition) abstractQueryCondition;
    }
    assert temporalQueryCondition != null;
    List<TimeLine> queryWindows = temporalQueryCondition.getQueryWindows();
    ArrayList<Trajectory> trajectories = new ArrayList<>();
    for (Trajectory trajectory : list) {
      TimeLine timeLine = new TimeLine(trajectory.getTrajectoryFeatures().getStartTime(), trajectory.getTrajectoryFeatures().getEndTime());
      for (TimeLine queryWindow : queryWindows) {
        if(queryWindow.intersect(timeLine)){
          trajectories.add(trajectory);
        }
      }
    }
    return trajectories;
  }

  @Override
  public IndexMeta findBestIndex() {
    Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
    // find a time index
    List<IndexMeta> indexList = null;
    if (map.containsKey(IndexType.Temporal)) {
      indexList = map.get(IndexType.Temporal);
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

  public static List<QueryCondition.TemporalQueryWindow> buildProtoTemporalWindows(
      TemporalQueryCondition temporalQueryCondition) {
    List<QueryCondition.TemporalQueryWindow> temporalQueryWindows = new ArrayList<>();
    for (TimeLine queryWindow : temporalQueryCondition.getQueryWindows()) {
      QueryCondition.TemporalQueryWindow temporalQueryWindow =
          QueryCondition.TemporalQueryWindow.newBuilder()
              .setStartMs(queryWindow.getTimeStart().toEpochSecond())
              .setEndMs(queryWindow.getTimeEnd().toEpochSecond())
              .build();
      temporalQueryWindows.add(temporalQueryWindow);
    }
    return temporalQueryWindows;
  }

  public static QueryCondition.TemporalQueryWindow buildProtoTemporalWindow(
      TemporalQueryCondition temporalQueryCondition) {
    if (temporalQueryCondition == null) return null;
    TimeLine queryWindow = temporalQueryCondition.getQueryWindows().get(0);

    return QueryCondition.TemporalQueryWindow.newBuilder()
        .setStartMs(queryWindow.getTimeStart().toEpochSecond())
        .setEndMs(queryWindow.getTimeEnd().toEpochSecond())
        .build();
  }
}
