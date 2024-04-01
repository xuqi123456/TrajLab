package whu.edu.cn.trajlab.db.condition;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
import whu.edu.cn.trajlab.db.enums.QueryType;

import java.io.IOException;
import java.io.Serializable;

/**
 * @author xuqi
 * @date 2024/02/17
 */
public class BufferQueryConditon extends AbstractQueryCondition implements Serializable {
    private final Trajectory centralTrajectory;
    private final double DisThreshold;

    public BufferQueryConditon(Trajectory centralTrajectory, double disThreshold) {
        this.centralTrajectory = centralTrajectory;
        DisThreshold = disThreshold;
    }

    public Trajectory getCentralTrajectory() {
        return centralTrajectory;
    }

    public double getDisThreshold() {
        return DisThreshold;
    }

    @Override
    public String getConditionInfo() {
        return "BufferQueryConditon{" +
                "centralTrajectory=" + centralTrajectory +
                ", DisThreshold=" + DisThreshold +
                '}';
    }
    public byte[] getTrajectoryBytes() throws IOException {
        return SerializerUtils.serializeObject(centralTrajectory);
    }
    public String getQueryWindowWKT(Geometry geometryWindow) {
        WKTWriter writer = new WKTWriter();
        return writer.write(geometryWindow);
    }

    @Override
    public QueryType getInputType() {
        return QueryType.BUFFER;
    }
}
