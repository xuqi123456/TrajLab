package whu.edu.cn.trajlab.db.condition;


import whu.edu.cn.trajlab.db.enums.QueryType;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class SpatialTemporalQueryCondition extends AbstractQueryCondition{

    private final SpatialQueryCondition spatialQueryCondition;
    private final TemporalQueryCondition temporalQueryCondition;

    public SpatialTemporalQueryCondition(SpatialQueryCondition spatialQueryCondition,
                                         TemporalQueryCondition temporalQueryCondition) {
        this.spatialQueryCondition = spatialQueryCondition;
        this.temporalQueryCondition = temporalQueryCondition;
    }

    public SpatialQueryCondition getSpatialQueryCondition() {
        return spatialQueryCondition;
    }

    public TemporalQueryCondition getTemporalQueryCondition() {
        return temporalQueryCondition;
    }

    @Override
    public String getConditionInfo() {
        return "SpatialTemporalQueryCondition{" +
                "spatialQueryCondition=" + spatialQueryCondition +
                ", temporalQueryCondition=" + temporalQueryCondition +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.ST;
    }
}
