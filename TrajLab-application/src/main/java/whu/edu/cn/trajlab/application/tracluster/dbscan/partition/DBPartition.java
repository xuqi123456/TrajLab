package whu.edu.cn.trajlab.application.tracluster.dbscan.partition;

import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import whu.edu.cn.trajlab.base.mbr.MinimumBoundingBox;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.GeoUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/01
 */
public class DBPartition implements Serializable {
  private final double minLen;
  private final Envelope envelope;
  private long xLen;
  private long yLen;

  public DBPartition(double minLen, Envelope envelope) {
    this.minLen = GeoUtils.getDegreeFromKm(minLen);
    this.envelope = envelope;
    calPartition();
  }

  public double getMinLen() {
    return minLen;
  }

  public Envelope getEnvelope() {
    return envelope;
  }

  public void calPartition() {
    xLen = (long) Math.ceil((envelope.getMaxX() - envelope.getMinX()) / minLen);
    yLen = (long) Math.ceil((envelope.getMaxY() - envelope.getMinY()) / minLen);
  }

  public List<Grid> calEnvelopeGridSet(Envelope envelope) {
    ArrayList<Grid> grids = new ArrayList<>();
    Grid gridLeftLow = calGridPartition(envelope.getMinX(), envelope.getMinY());
    Grid gridRightUp = calGridPartition(envelope.getMaxX(), envelope.getMaxY());
    for (long i = gridLeftLow.getX(); i <= gridRightUp.getX(); i++) {
      for (long j = gridLeftLow.getY(); j <= gridRightUp.getY(); j++) {
        grids.add(new Grid(i, j));
      }
    }
    return grids;
  }

  public List<Grid> calTrajectoryGridSet(Trajectory trajectory, MinimumBoundingBox traEnv) {
    LineString trajLine = trajectory.getLineString();
    Envelope envelopeInternal = trajLine.getEnvelopeInternal();
    List<Grid> gridSet = calEnvelopeGridSet(envelopeInternal);
    ArrayList<Grid> grids = new ArrayList<>();
    for (Grid grid : gridSet) {
      Envelope gridEnvelope =
          new Envelope(
              grid.getX() * minLen + traEnv.getMinX(),
              (grid.getX() + 1) * minLen + traEnv.getMinX(),
              grid.getY() * minLen + traEnv.getMinY(),
              (grid.getY() + 1) * minLen + traEnv.getMinY());
      Geometry envelopeGeometry = GeoUtils.createEnvelopeGeometry(gridEnvelope);
      if(trajLine.intersects(envelopeGeometry)){
        grids.add(grid);
      }
    }
    return grids;
  }

  public Grid calGridPartition(double x, double y) {
    long gridX = Math.min((int) ((x - envelope.getMinX()) / minLen), xLen - 1);
    long gridY = Math.min((int) ((y - envelope.getMinY()) / minLen), yLen - 1);
    return new Grid(gridX, gridY);
  }

  @Override
  public String toString() {
    return "DbPartition{"
        + "minLen="
        + minLen
        + ", envelope="
        + envelope
        + ", xLen="
        + xLen
        + ", yLen="
        + yLen
        + '}';
  }
}
