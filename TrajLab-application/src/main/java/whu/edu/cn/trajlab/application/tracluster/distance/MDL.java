package whu.edu.cn.trajlab.application.tracluster.distance;

import org.locationtech.jts.geom.Point;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.util.GeoUtils;

import java.util.List;

import static whu.edu.cn.trajlab.application.tracluster.constant.distanceContants.EPSILON;

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
        return Math.log(GeoUtils.getEuclideanDistanceM(startPoint, endPoint)) / Math.log(2);
    }
    public  double calMDLPair(int startIndex, int endIndex){
        Point startPoint = pointList.get(startIndex);
        Point endPoint = pointList.get(endIndex);
        double totalVerDis = 0;
        double totalAngDis = 0;
        for (int i = startIndex; i < endIndex; i++) {
            Point sPoint = pointList.get(i);
            Point ePoint = pointList.get(i + 1);
            if(checkSamePoint(i, i + 1)) continue;
            TraDistance traDistance = new TraDistance(sPoint, ePoint, startPoint, endPoint);
            totalVerDis += traDistance.getVerDis();
            totalAngDis += traDistance.getAngDis();
        }
        double lH = calLH(startIndex, endIndex);
        double lDH = Math.log(totalVerDis) / Math.log(2) + Math.log(totalAngDis) / Math.log(2);
        return lH + lDH;
    }
    public double calMDLNoPair(int startIndex, int endIndex){
        double totalDis = 0;
        for (int i = startIndex; i < endIndex; i++) {
            Point sPoint = pointList.get(i);
            Point ePoint = pointList.get(i + 1);
            if(checkSamePoint(i, i + 1)) continue;
            totalDis += Math.log(GeoUtils.getEuclideanDistanceM(sPoint, ePoint)) / Math.log(2);
        }
        return totalDis;
    }
    public boolean calDistanceCompareRadius(int currentIndex, double radius){
        TrajPoint currentP = pointList.get(currentIndex);
        TrajPoint lastP = pointList.get(currentIndex - 1);
        return GeoUtils.getEuclideanDistanceKM(currentP, lastP) > radius;
    }

    public boolean checkSamePoint(int startIndex, int endIndex){
        TrajPoint sPoint = pointList.get(startIndex);
        TrajPoint ePoint = pointList.get(endIndex);
        return GeoUtils.getEuclideanDistanceKM(sPoint, ePoint) < EPSILON;
    }
}
