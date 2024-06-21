package whu.edu.cn.trajlab.application.tracluster.segment;

import org.apache.spark.api.java.JavaRDD;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.enums.TimePeriod;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/04/12
 */
public class TimeSegment implements Serializable {
    private final double maxTimeInterval;
    private final TimePeriod timePeriod;

    public TimeSegment(double maxTimeInterval, TimePeriod timePeriod) {
        this.maxTimeInterval = maxTimeInterval;
        this.timePeriod = timePeriod;
    }

    public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
        List<TrajPoint> tmpPointList = rawTrajectory.getPointList();
        List<Trajectory> res = new ArrayList<>();
        List<Integer> segIndex = new ArrayList<>();
        segIndex.add(0);
        int startIndex = 0;
        int length = 2;
        while(startIndex + length < tmpPointList.size()){
            int currentIndex = startIndex + length;
            TrajPoint p0 = tmpPointList.get(startIndex), p1 = tmpPointList.get(currentIndex);
            if (timePeriod.getChronoUnit().between(p0.getTimestamp(), p1.getTimestamp()) >= maxTimeInterval) {
                // record segIndex
                segIndex.add(currentIndex - 1);
                startIndex = currentIndex - 1;
                length = 2;
            }else length++;
        }
        segIndex.add(tmpPointList.size() - 1);
        // do segment
        for (int i = 0; i < segIndex.size() - 1; i++) {
            List<TrajPoint> tmpPts =
                    new ArrayList<>(tmpPointList.subList(segIndex.get(i), segIndex.get(i + 1)));
            tmpPts.add(tmpPointList.get(segIndex.get(i + 1)));
            Trajectory tmp = SegmentUtils.genNewTrajectory(
                    rawTrajectory.getTrajectoryID(),
                    rawTrajectory.getObjectID(),
                    tmpPts,
                    rawTrajectory.getExtendedValues(),
                    0);
            if (tmp == null) {
                continue;
            }
            res.add(tmp);
        }
        return res;
    }

    public JavaRDD<Trajectory> segment(JavaRDD<Trajectory> rawTrajectoryRDD) {
        return rawTrajectoryRDD.flatMap(item -> segmentFunction(item).iterator()).filter(Objects::nonNull);
    }
}
