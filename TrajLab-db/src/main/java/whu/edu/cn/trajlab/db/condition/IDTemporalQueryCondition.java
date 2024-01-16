package whu.edu.cn.trajlab.db.condition;

import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.QueryType;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;

import java.util.List;

/**
 * @author xuqi
 * @date 2023/12/10
 */
public class IDTemporalQueryCondition extends AbstractQueryCondition {

  private List<TimeLine> queryWindows;
  private TemporalQueryType temporalQueryType;
  private IDQueryCondition idQueryCondition;

  public IDTemporalQueryCondition(
      List<TimeLine> queryWindows,
      TemporalQueryType temporalQueryType,
      IDQueryCondition idQueryCondition) {
    this.queryWindows = queryWindows;
    this.temporalQueryType = temporalQueryType;
    this.idQueryCondition = idQueryCondition;
  }

  public List<TimeLine> getQueryWindows() {
    return queryWindows;
  }

  public TemporalQueryType getTemporalQueryType() {
    return temporalQueryType;
  }

  public IDQueryCondition getIdQueryCondition() {
    return idQueryCondition;
  }

  @Override
  public String getConditionInfo() {
    return "IDTemporalQueryCondition{" +
            "queryWindows=" + queryWindows +
            ", temporalQueryType=" + temporalQueryType +
            ", idQueryCondition=" + idQueryCondition +
            '}';
  }

  @Override
  public QueryType getInputType() {
    return QueryType.ID_T;
  }
}
