package whu.edu.cn.trajlab.db.condition;

import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.QueryType;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;

import java.io.Serializable;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/12/10
 */
public class IDTemporalQueryCondition extends AbstractQueryCondition implements Serializable {

  private TemporalQueryCondition temporalQueryCondition;
  private IDQueryCondition idQueryCondition;

  public IDTemporalQueryCondition(TemporalQueryCondition temporalQueryCondition, IDQueryCondition idQueryCondition) {
    this.temporalQueryCondition = temporalQueryCondition;
    this.idQueryCondition = idQueryCondition;
  }

  public TemporalQueryCondition getTemporalQueryCondition() {
    return temporalQueryCondition;
  }

  public IDQueryCondition getIdQueryCondition() {
    return idQueryCondition;
  }

  @Override
  public String getConditionInfo() {
    return "IDTemporalQueryCondition{" +
            "temporalQueryCondition=" + temporalQueryCondition +
            ", idQueryCondition=" + idQueryCondition +
            '}';
  }

  @Override
  public QueryType getInputType() {
    return QueryType.ID_T;
  }
}
