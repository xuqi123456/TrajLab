package whu.edu.cn.trajlab.db.condition;

import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.QueryType;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class TemporalQueryCondition extends AbstractQueryCondition implements Serializable {

    private List<TimeLine> queryWindows;
    private final TemporalQueryType temporalQueryType;

    public TemporalQueryCondition(TimeLine queryWindow,
                                  TemporalQueryType temporalQueryType) {
        this.queryWindows = Collections.singletonList(queryWindow);
        this.temporalQueryType = temporalQueryType;
    }

    public TemporalQueryCondition(List<TimeLine> queryWindows,
                                  TemporalQueryType temporalQueryType) {
        this.queryWindows = queryWindows;
        this.temporalQueryType = temporalQueryType;
    }

    public TemporalQueryType getTemporalQueryType() {
        return temporalQueryType;
    }

    public List<TimeLine> getQueryWindows() {
        return queryWindows;
    }

    public boolean validate(TimeLine timeLine) {
        for (TimeLine queryWindow : queryWindows) {
            if (temporalQueryType == TemporalQueryType.CONTAIN ?
                    queryWindow.contain(timeLine) : queryWindow.intersect(timeLine)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getConditionInfo() {
        return "TemporalQueryCondition{" +
                "queryWindows=" + queryWindows +
                ", temporalQueryType=" + temporalQueryType +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.TEMPORAL;
    }
}

