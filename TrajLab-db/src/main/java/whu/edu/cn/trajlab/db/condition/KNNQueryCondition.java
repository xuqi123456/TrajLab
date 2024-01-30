package whu.edu.cn.trajlab.db.condition;

import org.locationtech.jts.io.WKTWriter;
import whu.edu.cn.trajlab.base.point.BasePoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.db.enums.KNNQueryType;
import whu.edu.cn.trajlab.db.enums.QueryType;

import java.io.IOException;

/**
 * @author xuqi
 * @date 2024/01/24
 */
public class KNNQueryCondition extends AbstractQueryCondition{
    private final int k;
    private BasePoint centralPoint;
    private Trajectory centralTrajectory;
    private TemporalQueryCondition temporalQueryCondition;
    private final KNNQueryType knnQueryType;

    public KNNQueryCondition(int k, Trajectory centralTrajectory, TemporalQueryCondition temporalQueryCondition) {
        this.k = k;
        this.centralTrajectory = centralTrajectory;
        this.temporalQueryCondition = temporalQueryCondition;
        this.knnQueryType = KNNQueryType.Trajectory;
    }

    public KNNQueryCondition(int k, BasePoint centralPoint, TemporalQueryCondition temporalQueryCondition) {
        this.k = k;
        this.centralPoint = centralPoint;
        this.temporalQueryCondition = temporalQueryCondition;
        this.knnQueryType = KNNQueryType.Point;
    }

    public KNNQueryCondition(int k, Trajectory centralTrajectory) {
        this.k = k;
        this.centralTrajectory = centralTrajectory;
        this.knnQueryType = KNNQueryType.Trajectory;
    }

    public KNNQueryCondition(int k, BasePoint centralPoint) {
        this.k = k;
        this.centralPoint = centralPoint;
        this.knnQueryType = KNNQueryType.Point;
    }

    public int getK() {
        return k;
    }

    public BasePoint getCentralPoint() {
        return centralPoint;
    }

    public Trajectory getCentralTrajectory() {
        return centralTrajectory;
    }

    public TemporalQueryCondition getTemporalQueryCondition() {
        return temporalQueryCondition;
    }

    public KNNQueryType getKnnQueryType() {
        return knnQueryType;
    }
    public byte[] getPointBytes() throws IOException {
        return SerializerUtils.serializeObject(centralPoint);
    }
    public byte[] getTrajectoryBytes() throws IOException {
        return SerializerUtils.serializeObject(centralTrajectory);
    }

    @Override
    public String getConditionInfo() {
        return "KNNQueryCondition{" +
                "k=" + k +
                ", centralPoint=" + centralPoint +
                ", centralTrajectory=" + centralTrajectory +
                ", temporalQueryCondition=" + temporalQueryCondition +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.KNN;
    }
}
