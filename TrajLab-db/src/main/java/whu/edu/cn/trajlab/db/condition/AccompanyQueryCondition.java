package whu.edu.cn.trajlab.db.condition;

import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.db.enums.QueryType;
import whu.edu.cn.trajlab.db.enums.TimePeriod;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2024/02/18
 */
public class AccompanyQueryCondition extends AbstractQueryCondition{

    private final Trajectory centralTrajectory;
    private final double DisThreshold;
    private final double TimeThreshold;
    private TimePeriod timePeriod = TimePeriod.DAY;
    private final int k;

    public AccompanyQueryCondition(Trajectory centralTrajectory, double disThreshold, double timeThreshold, int k) {
        this.centralTrajectory = centralTrajectory;
        DisThreshold = disThreshold;
        TimeThreshold = timeThreshold;
        this.k = k;
    }

    public AccompanyQueryCondition(Trajectory centralTrajectory, double disThreshold, double timeThreshold, TimePeriod timePeriod, int k) {
        this.centralTrajectory = centralTrajectory;
        DisThreshold = disThreshold;
        TimeThreshold = timeThreshold;
        this.timePeriod = timePeriod;
        this.k = k;
    }

    public Trajectory getCentralTrajectory() {
        return centralTrajectory;
    }

    public byte[] getTrajectoryBytes() throws IOException {
        return SerializerUtils.serializeObject(centralTrajectory);
    }
    public double getDisThreshold() {
        return DisThreshold;
    }

    public double getTimeThreshold() {
        return TimeThreshold;
    }

    public TimePeriod getTimePeriod() {
        return timePeriod;
    }

    public int getK() {
        return k;
    }

    @Override
    public String getConditionInfo() {
        return "AccompanyQueryCondition{" +
                "centralTrajectory=" + centralTrajectory +
                ", DisThreshold=" + DisThreshold +
                ", TimeThreshold=" + TimeThreshold +
                ", timePeriod=" + timePeriod +
                ", k=" + k +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.ACCOMPANY;
    }
}
