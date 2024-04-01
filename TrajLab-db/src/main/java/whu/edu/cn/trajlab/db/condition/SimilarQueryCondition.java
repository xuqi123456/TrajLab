package whu.edu.cn.trajlab.db.condition;

import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.db.enums.QueryType;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author xuqi
 * @date 2024/01/29
 */
public class SimilarQueryCondition extends AbstractQueryCondition implements Serializable {
    private final Trajectory centralTrajectory;
    private final double SimDisThreshold;
    private TemporalQueryCondition temporalQueryCondition;

    public SimilarQueryCondition(Trajectory centralTrajectory, double simDisThreshold, TemporalQueryCondition temporalQueryCondition) {
        this.centralTrajectory = centralTrajectory;
        SimDisThreshold = simDisThreshold;
        this.temporalQueryCondition = temporalQueryCondition;
    }

    public SimilarQueryCondition(Trajectory centralTrajectory, double simDisThreshold) {
        this.centralTrajectory = centralTrajectory;
        SimDisThreshold = simDisThreshold;
    }

    public Trajectory getCentralTrajectory() {
        return centralTrajectory;
    }

    public double getSimDisThreshold() {
        return SimDisThreshold;
    }

    public TemporalQueryCondition getTemporalQueryCondition() {
        return temporalQueryCondition;
    }

    public byte[] getTrajectoryBytes() throws IOException {
        return SerializerUtils.serializeObject(centralTrajectory);
    }


    @Override
    public String getConditionInfo() {
        return "SimilarQueryCondition{" +
                "centralTrajectory=" + centralTrajectory +
                ", SimDisThreshold=" + SimDisThreshold +
                ", temporalQueryCondition=" + temporalQueryCondition +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.SIMILAR;
    }
}
