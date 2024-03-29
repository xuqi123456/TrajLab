package whu.edu.cn.trajlab.db.coding.coding;

import whu.edu.cn.trajlab.db.coding.sfc.SFCRange;
import whu.edu.cn.trajlab.db.coding.sfc.XZ2SFC;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.constant.CodingConstants;
import whu.edu.cn.trajlab.db.constant.IndexConstants;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static whu.edu.cn.trajlab.db.constant.IndexConstants.DEFAULT_range_NUM;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class XZ2Coding implements SpatialCoding {

  private static final Logger logger = LoggerFactory.getLogger(XZ2Coding.class);

  public static final int BYTES = Long.BYTES;

  private XZ2SFC xz2Sfc;

  short xz2Precision;

  public XZ2Coding() {
    xz2Precision = CodingConstants.MAX_XZ2_PRECISION;
    xz2Sfc = XZ2SFC.getInstance(xz2Precision);
  }

  public XZ2SFC getXz2Sfc() {
    return xz2Sfc;
  }

  /**
   * Get xz2 index for the line string.
   *
   * @param lineString Line string to be indexed.
   * @return The XZ2 code
   */
  public ByteArray code(LineString lineString) {
    Envelope boundingBox = lineString.getEnvelopeInternal();
    double minLng = boundingBox.getMinX();
    double maxLng = boundingBox.getMaxX();
    double minLat = boundingBox.getMinY();
    double maxLat = boundingBox.getMaxY();
    // lenient is false so the points out of boundary can throw exception.
    ByteBuffer br = ByteBuffer.allocate(Long.BYTES);
    br.putLong(xz2Sfc.index(minLng, maxLng, minLat, maxLat, false));
    return new ByteArray(br);
  }

  /**
   * Get index ranges of the query range, support two spatial query types
   *
   * @param spatialQueryCondition Spatial query on the index.
   * @return List of xz2 index ranges corresponding to the query range.
   */
  public List<CodingRange> ranges(SpatialQueryCondition spatialQueryCondition) {
    Envelope envelope = spatialQueryCondition.getQueryWindow();
    List<CodingRange> codingRangeList = new LinkedList<>();
    List<SFCRange> sfcRangeList =
        xz2Sfc.ranges(
            envelope,
            spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN);
    for (SFCRange sfcRange : sfcRangeList) {
      CodingRange codingRange = new CodingRange();
      codingRange.concatSfcRange(sfcRange);
      codingRangeList.add(codingRange);
    }
    return codingRangeList;
  }

  public List<CodingRange> rangesWithPartition(SpatialQueryCondition spatialQueryCondition) {
    Envelope envelope = spatialQueryCondition.getQueryWindow();
    List<CodingRange> codingRangeList = new LinkedList<>();
    List<SFCRange> sfcRangeList =
        xz2Sfc.ranges(
            envelope,
            spatialQueryCondition.getQueryType() == SpatialQueryCondition.SpatialQueryType.CONTAIN);
    List<SFCRange> sfcRanges = splitPartitionSFCRange(sfcRangeList);
    for (SFCRange sfcRange : sfcRanges) {
      CodingRange codingRange = new CodingRange();
      codingRange.concatSfcRange(sfcRange);
      codingRangeList.add(codingRange);
    }
    return codingRangeList;
  }

  public List<SFCRange> splitPartitionSFCRange(List<SFCRange> sfcRanges) {
    ArrayList<SFCRange> ranges = new ArrayList<>();
    for (SFCRange sfcRange : sfcRanges) {
      long low = sfcRange.lower;
      long up = sfcRange.upper;
      ByteArray cShardKey = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(low));
      short cShard = (short) Math.abs(cShardKey.hashCode() / DEFAULT_range_NUM % IndexConstants.DEFAULT_SHARD_NUM);
      for (long key = sfcRange.lower + 1; key <= sfcRange.upper; key++){
        ByteArray shardKey = new ByteArray(ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(key));
        short shard = (short) Math.abs(shardKey.hashCode() / DEFAULT_range_NUM % IndexConstants.DEFAULT_SHARD_NUM);
        if(shard == cShard){
          continue;
        }else {
          ranges.add(new SFCRange(low, key-1, sfcRange.validated));
          low = key;
          cShard = shard;
        }
      }
      ranges.add(new SFCRange(low, up, sfcRange.validated));
    }
    return ranges;
  }

  @Override
  public Polygon getCodingPolygon(ByteArray spatialCodingByteArray) {
    ByteBuffer br = spatialCodingByteArray.toByteBuffer();
    ((Buffer) br).flip();
    long coding = br.getLong();
    return xz2Sfc.getEnlargedRegion(coding);
  }

  @Override
  public String toString() {
    return "XZ2Coding{" + "xz2Precision=" + xz2Precision + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    XZ2Coding xz2Coding = (XZ2Coding) o;
    return xz2Precision == xz2Coding.xz2Precision;
  }

  @Override
  public int hashCode() {
    return Objects.hash(xz2Precision);
  }
}
