package whu.edu.cn.trajlab.application.tracluster.segment;

import org.apache.spark.api.java.JavaRDD;
import whu.edu.cn.trajlab.application.tracluster.distance.MDL;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/02/28
 */
public class MdlSegment implements Serializable {
    private double minLns = 0;

    public MdlSegment(double minLns) {
        this.minLns = minLns;
    }

    public MdlSegment() {
    }

    public double getMinLns() {
        return minLns;
    }

    public List<Trajectory> segmentFunction(Trajectory rawTrajectory) {
        List<TrajPoint> tmpPointList = rawTrajectory.getPointList();
        List<Trajectory> res = new ArrayList<>();
        List<Integer> segIndex = new ArrayList<>();
        int startIndex = 0;
        int length = 1;
        //记录特征点的位置
        segIndex.add(0);
        MDL mdl = new MDL(tmpPointList);
        while(startIndex + length < tmpPointList.size()){
            int currentIndex = startIndex + length;
            double cosPair = mdl.calMDLPair(startIndex, currentIndex);
            double cosNoPair = mdl.calMDLNoPair(startIndex, currentIndex);
            if(cosPair > cosNoPair){
                segIndex.add(currentIndex - 1);
                startIndex = currentIndex - 1;
                length = 1;
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
                    minLns);
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
