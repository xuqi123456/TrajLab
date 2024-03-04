package whu.edu.cn.trajlab.application.Tracluster.distance;

import org.locationtech.jts.geom.*;
import org.locationtech.jts.math.Vector2D;

/**
 * @author xuqi
 * @date 2024/02/28
 */
public class TraDistance {
  private final Vector2D lineString1;
  private final Vector2D lineString2;
  private double verDis;
  private final double angDis;
  GeometryFactory geometryFactory = new GeometryFactory(new PrecisionModel(), 4326);

  public TraDistance(Vector2D lineString1, Vector2D lineString2) {
    this.lineString1 = lineString1;
    this.lineString2 = lineString2;
    this.angDis = calAngleDistance();
  }

  public TraDistance(double x1, double y1, double x2, double y2) {
    this.lineString1 = new Vector2D(x1, y1);
    this.lineString2 = new Vector2D(x2, y2);
    this.angDis = calAngleDistance();
  }

  public TraDistance(Point l1Start, Point l1End, Point l2Start, Point l2End) {

    LineString line1 =
        geometryFactory.createLineString(
            new Coordinate[] {l1Start.getCoordinate(), l1End.getCoordinate()});
    LineString line2 =
        geometryFactory.createLineString(
            new Coordinate[] {l2Start.getCoordinate(), l2End.getCoordinate()});
    if (line1.getLength() > line2.getLength()) {
      LineString temp = line1;
      line1 = line2;
      line2 = temp;
    }
    this.verDis = calVerticalDistance(line1, line2);

    this.lineString1 =
            new Vector2D(
                    Math.abs(l1End.getX() - l1Start.getX()), Math.abs(l1End.getY() - l1Start.getY()));
    this.lineString2 =
            new Vector2D(
                    Math.abs(l2End.getX() - l2Start.getX()), Math.abs(l2End.getY() - l2Start.getY()));
    this.angDis = calAngleDistance();
  }

  public Vector2D getLine1() {
    return lineString1;
  }

  public Vector2D getLine2() {
    return lineString2;
  }

  public double getVerDis() {
    return verDis;
  }

  public double getAngDis() {
    return angDis;
  }

  @Override
  public String toString() {
    return "TraDistance{" +
            "lineString1=" + lineString1 +
            ", lineString2=" + lineString2 +
            ", verDis=" + verDis +
            ", angDis=" + angDis +
            '}';
  }

  public double calVerticalDistance(LineString line1, LineString line2) {
    Coordinate sPointCoordinate = line1.getCoordinate();
    Point sPoint = geometryFactory.createPoint(sPointCoordinate);
    double dis1 = sPoint.distance(line2);
    Coordinate ePointCoordinate = line1.getCoordinateN(line1.getNumPoints() - 1);
    Point ePoint = geometryFactory.createPoint(ePointCoordinate);

    double dis2 = ePoint.distance(line2);

    return (Math.pow(dis1, 2) + Math.pow(dis2, 2)) / (dis1 + dis2);
  }

  public double calAngleDistance() {
    // 计算叉乘
    double crossProduct = lineString1.getX() * lineString2.getY() - lineString2.getX() * lineString1.getY();

    // 计算向量AB和向量CD的模长
    double lengthAB = Math.sqrt(lineString1.getX() * lineString1.getX() + lineString1.getY() * lineString1.getY());
    double lengthCD = Math.sqrt(lineString2.getX() * lineString2.getX() + lineString2.getY() * lineString2.getY());

    // 计算夹角的sin值
    double sinValue = Math.abs(crossProduct / (lengthAB * lengthCD));
    return Math.min(lengthAB, lengthCD) * sinValue;
  }
}
