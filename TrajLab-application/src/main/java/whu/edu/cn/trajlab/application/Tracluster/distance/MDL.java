package whu.edu.cn.trajlab.application.Tracluster.distance;

import org.locationtech.jts.geom.Point;
import whu.edu.cn.trajlab.base.point.TrajPoint;

import java.util.List;

/**
 * @author xuqi
 * @date 2024/02/28
 */
public class MDL {
    private final List<TrajPoint> pointList;

    public MDL(List<TrajPoint> pointList) {
        this.pointList = pointList;
    }

    public List<TrajPoint> getPointList() {
        return pointList;
    }
    public double calLH(int startIndex, int endIndex){
        Point startPoint = pointList.get(startIndex);
        Point endPoint = pointList.get(endIndex);
        return Math.log(startPoint.distance(endPoint)) / Math.log(2);
    }
    public  double calMDLPair(int startIndex, int endIndex){
        Point startPoint = pointList.get(startIndex);
        Point endPoint = pointList.get(endIndex);
        double totalVerDis = 0;
        double totalAngDis = 0;
        for (int i = startIndex; i < endIndex; i++) {
            Point sPoint = pointList.get(i);
            Point ePoint = pointList.get(i + 1);
            TraDistance traDistance = new TraDistance(sPoint, ePoint, startPoint, endPoint);
            totalVerDis += traDistance.getVerDis();
            totalAngDis += traDistance.getAngDis();
        }
        return Math.log(totalVerDis) / Math.log(2) + Math.log(totalAngDis) / Math.log(2);
    }
    public double calMDLNoPair(int startIndex, int endIndex){
        double totalDis = 0;
        for (int i = startIndex; i < endIndex; i++) {
            Point sPoint = pointList.get(i);
            Point ePoint = pointList.get(i + 1);
            totalDis += Math.log(sPoint.distance(ePoint)) / Math.log(2);
        }
        return totalDis;
    }
}
