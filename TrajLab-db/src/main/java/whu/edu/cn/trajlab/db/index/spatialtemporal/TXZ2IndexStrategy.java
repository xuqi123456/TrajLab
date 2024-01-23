package whu.edu.cn.trajlab.db.index.spatialtemporal;

import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.SpatialQueryCondition;
import whu.edu.cn.trajlab.db.condition.SpatialTemporalQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeBin;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.IndexStrategy;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import org.apache.hadoop.hbase.util.Bytes;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.coding.coding.*;
import whu.edu.cn.trajlab.db.constant.CodingConstants;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.IndexConstants.DEFAULT_range_NUM;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class TXZ2IndexStrategy extends IndexStrategy {

  private final XZ2Coding xz2Coding;
  private final XZTCoding xztCoding;

  public TXZ2IndexStrategy(XZTCoding xztCoding, XZ2Coding xz2Coding) {
    indexType = IndexType.TXZ2;
    this.xztCoding = xztCoding;
    this.xz2Coding = xz2Coding;
  }

  public TXZ2IndexStrategy() {
    indexType = IndexType.TXZ2;
    this.xztCoding = new XZTCoding();
    this.xz2Coding = new XZ2Coding();
  }

  /** 作为行键时的字节数 */
  private static final int PHYSICAL_KEY_BYTE_LEN =
      Short.BYTES
          + XZTCoding.BYTES_NUM
          + XZ2Coding.BYTES
          + CodingConstants.MAX_OID_LENGTH
          + CodingConstants.MAX_TID_LENGTH;

  private static final int LOGICAL_KEY_BYTE_LEN =
      PHYSICAL_KEY_BYTE_LEN - Short.BYTES - CodingConstants.MAX_TID_LENGTH;
  private static final int PARTITION_KEY_BYTE_LEN = XZTCoding.BYTES_NUM;
  private static final int SCAN_RANGE_BYTE_LEN =
      PHYSICAL_KEY_BYTE_LEN - CodingConstants.MAX_OID_LENGTH - CodingConstants.MAX_TID_LENGTH;

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine =
        new TimeLine(
            trajectory.getTrajectoryFeatures().getStartTime(),
            trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.index(timeLine);
    byte[] bytesEnd = trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN + bytesEnd.length);
    byteBuffer.put(timeCode.getBytes());
    byteBuffer.put(spatialCoding.getBytes());
    byteBuffer.put(getObjectIDBytes(trajectory));
    byteBuffer.put(getTrajectoryIDBytes(trajectory));
    return new ByteArray(byteBuffer);
  }

  @Override
  protected ByteArray partitionIndex(Trajectory trajectory) {
    TimeLine timeLine =
        new TimeLine(
            trajectory.getTrajectoryFeatures().getStartTime(),
            trajectory.getTrajectoryFeatures().getEndTime());
    ByteArray timeCode = xztCoding.index(timeLine);
    ByteBuffer byteBuffer = ByteBuffer.allocate(PARTITION_KEY_BYTE_LEN);
    byteBuffer.put(timeCode.getBytes());
    return new ByteArray(byteBuffer);
  }

  private ByteArray toRowKeyRangeBoundary(
      short shard, ByteArray timeBytes, ByteArray xz2Bytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(timeBytes.getBytes());
    if (end) {
      long xz2code = Bytes.toLong(xz2Bytes.getBytes()) + 1;
      byteBuffer.putLong(xz2code);
    } else {
      byteBuffer.put(xz2Bytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return xztCoding.getXZTElementTimeLine(Bytes.toLong(byteArray.getBytes()));
  }

  @Override
  public List<RowKeyRange> getScanRanges(AbstractQueryCondition abstractQueryCondition) {
    if (abstractQueryCondition instanceof SpatialTemporalQueryCondition) {
      SpatialTemporalQueryCondition spatialTemporalQueryCondition =
          (SpatialTemporalQueryCondition) abstractQueryCondition;
      if (spatialTemporalQueryCondition.getTemporalQueryCondition() == null) {
        throw new UnsupportedOperationException();
      }
      List<RowKeyRange> result = new ArrayList<>();
      SpatialQueryCondition spatialQueryCondition =
          spatialTemporalQueryCondition.getSpatialQueryCondition();
      TemporalQueryCondition temporalQueryCondition =
          spatialTemporalQueryCondition.getTemporalQueryCondition();
      List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
      // 四重循环，所有可能的时间编码都应单独取值
      for (CodingRange temporalCodingRange : temporalCodingRanges) {
        long lowerXZTCode = Bytes.toLong(temporalCodingRange.getLower().getBytes());
        long upperXZTCode = Bytes.toLong(temporalCodingRange.getUpper().getBytes());
        boolean tValidate = temporalCodingRange.isValidated();
        List<CodingRange> spatialCodingRanges = xz2Coding.ranges(spatialQueryCondition);
        for (long xztCode = lowerXZTCode; xztCode <= upperXZTCode; xztCode++) {
          for (CodingRange spatialCodingRange : spatialCodingRanges) {
            boolean sValidate = spatialCodingRange.isValidated();
            for (short shard = 0; shard < shardNum; shard++) {
              ByteArray byteArray1 =
                  toRowKeyRangeBoundary(
                      shard,
                      new ByteArray(Bytes.toBytes(xztCode)),
                      spatialCodingRange.getLower(),
                      false);
              ByteArray byteArray2 =
                  toRowKeyRangeBoundary(
                      shard,
                      new ByteArray(Bytes.toBytes(xztCode)),
                      spatialCodingRange.getUpper(),
                      true);
              result.add(new RowKeyRange(byteArray1, byteArray2, tValidate && sValidate));
            }
          }
        }
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public List<RowKeyRange> getPartitionScanRanges(AbstractQueryCondition abstractQueryCondition) {
    if (abstractQueryCondition instanceof SpatialTemporalQueryCondition) {
      SpatialTemporalQueryCondition spatialTemporalQueryCondition =
          (SpatialTemporalQueryCondition) abstractQueryCondition;
      if (spatialTemporalQueryCondition.getTemporalQueryCondition() == null) {
        throw new UnsupportedOperationException();
      }
      List<RowKeyRange> result = new ArrayList<>();
      SpatialQueryCondition spatialQueryCondition =
          spatialTemporalQueryCondition.getSpatialQueryCondition();
      TemporalQueryCondition temporalQueryCondition =
          spatialTemporalQueryCondition.getTemporalQueryCondition();
      List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
      // 四重循环，所有可能的时间编码都应单独取值
      for (CodingRange temporalCodingRange : temporalCodingRanges) {
        long lowerXZTCode = Bytes.toLong(temporalCodingRange.getLower().getBytes());
        long upperXZTCode = Bytes.toLong(temporalCodingRange.getUpper().getBytes());
        boolean tValidate = temporalCodingRange.isValidated();
        List<CodingRange> spatialCodingRanges = xz2Coding.ranges(spatialQueryCondition);
        for (long xztCode = lowerXZTCode; xztCode <= upperXZTCode; xztCode++) {
          short shard =
              (short)
                  Math.abs(
                      (temporalCodingRange.byteSfcCode(xztCode).hashCode()
                          / DEFAULT_range_NUM
                          % shardNum));
          for (CodingRange spatialCodingRange : spatialCodingRanges) {
            boolean sValidate = spatialCodingRange.isValidated();
            ByteArray byteArray1 =
                toRowKeyRangeBoundary(
                    shard,
                    new ByteArray(Bytes.toBytes(xztCode)),
                    spatialCodingRange.getLower(),
                    false);
            ByteArray byteArray2 =
                toRowKeyRangeBoundary(
                    shard,
                    new ByteArray(Bytes.toBytes(xztCode)),
                    spatialCodingRange.getUpper(),
                    true);
            result.add(new RowKeyRange(byteArray1, byteArray2, tValidate && sValidate, shard));
          }
        }
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {"
        + "shardNum="
        + getShardNum(byteArray)
        + ", bin = "
        + getTimeBin(byteArray)
        + ", timeCoding = "
        + getTimeElementCode(byteArray)
        + ", xz2="
        + extractSpatialCode(byteArray)
        + ", oidAndTid="
        + getObjectID(byteArray)
        + "-"
        + getTrajectoryID(byteArray)
        + '}';
  }

  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
  }

  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort(); // shard
    buffer.getLong(); // time code
    byte[] bytes = new byte[XZ2Coding.BYTES];
    buffer.get(bytes);
    return new ByteArray(bytes);
  }

  public TimeCoding getTimeCoding() {
    return xztCoding;
  }

  public long getTimeCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    return buffer.getLong();
  }

  public TimeBin getTimeBin(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    long xztCode = buffer.getLong();
    return xztCoding.getTimeBin(xztCode);
  }

  public long getTimeElementCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    long xztCode = buffer.getLong();
    return xztCoding.getElementCode(xztCode);
  }

  @Override
  public short getShardNum(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    return buffer.getShort();
  }

  @Override
  public String getObjectID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    buffer.getLong();
    byte[] stringBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(stringBytes);
    return new String(stringBytes, StandardCharsets.UTF_8);
  }

  @Override
  public String getTrajectoryID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    buffer.getLong();
    byte[] oidBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(oidBytes);
    int validTidLength = buffer.remaining();
    byte[] validTidBytes = new byte[validTidLength];
    buffer.get(validTidBytes);
    return new String(validTidBytes, StandardCharsets.UTF_8);
  }
}
