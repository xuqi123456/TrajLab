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
public class XZ2TIndexStrategy extends IndexStrategy {

  private final XZ2Coding xz2Coding;
  private final XZTCoding xztCoding;

  public XZ2TIndexStrategy(XZ2Coding xz2Coding, XZTCoding xztCoding) {
    indexType = IndexType.XZ2T;
    this.xz2Coding = xz2Coding;
    this.xztCoding = xztCoding;
  }

  public XZ2TIndexStrategy() {
    indexType = IndexType.XZ2T;
    this.xz2Coding = new XZ2Coding();
    this.xztCoding = new XZTCoding();
  }

  private static final int PHYSICAL_KEY_BYTE_LEN =
      Short.BYTES
          + XZ2Coding.BYTES
          + XZTCoding.BYTES_NUM
          + CodingConstants.MAX_OID_LENGTH
          + CodingConstants.MAX_TID_LENGTH;
  private static final int LOGICAL_KEY_BYTE_LEN =
      PHYSICAL_KEY_BYTE_LEN - Short.BYTES - CodingConstants.MAX_TID_LENGTH;
  private static final int PARTITION_KEY_BYTE_LEN = XZ2Coding.BYTES;
  private static final int SCAN_RANGE_BYTE_LEN =
      PHYSICAL_KEY_BYTE_LEN - CodingConstants.MAX_OID_LENGTH - CodingConstants.MAX_TID_LENGTH;

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    TimeLine timeLine =
        new TimeLine(
            trajectory.getTrajectoryFeatures().getStartTime(),
            trajectory.getTrajectoryFeatures().getEndTime());
    byte[] bytesEnd = trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8);
    ByteArray timeCode = xztCoding.index(timeLine);
    ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN + bytesEnd.length);
    byteBuffer.put(spatialCoding.getBytes());
    byteBuffer.put(timeCode.getBytes());
    byteBuffer.put(getObjectIDBytes(trajectory));
    byteBuffer.put(getTrajectoryIDBytes(trajectory));
    return new ByteArray(byteBuffer);
  }

  @Override
  protected ByteArray partitionIndex(Trajectory trajectory) {
    ByteArray spatialCoding = xz2Coding.code(trajectory.getLineString());
    ByteBuffer byteBuffer = ByteBuffer.allocate(PARTITION_KEY_BYTE_LEN);
    byteBuffer.put(spatialCoding.getBytes());
    return new ByteArray(byteBuffer);
  }

  private ByteArray toRowKeyRangeBoundary(
      short shard, ByteArray xz2Bytes, ByteArray timeBytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(xz2Bytes.getBytes());
    if (end) {
      byteBuffer.putLong(Bytes.toLong(timeBytes.getBytes()) + 1);
    } else {
      byteBuffer.put(timeBytes.getBytes());
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
      List<RowKeyRange> result = new ArrayList<>();
      SpatialQueryCondition spatialQueryCondition =
              spatialTemporalQueryCondition.getSpatialQueryCondition();
      TemporalQueryCondition temporalQueryCondition =
              spatialTemporalQueryCondition.getTemporalQueryCondition();
      // 四重循环，所有可能的时间编码都应单独取值
      for (CodingRange spatialCodingRange : xz2Coding.ranges(spatialQueryCondition)) {
        long lowerXZ2Code = Bytes.toLong(spatialCodingRange.getLower().getBytes());
        long upperXZ2Code = Bytes.toLong(spatialCodingRange.getUpper().getBytes());
        boolean sValidate = spatialCodingRange.isValidated();
        List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
        for (long xzCode = lowerXZ2Code; xzCode <= upperXZ2Code; xzCode++) {
          for (CodingRange temporalCodingRange : temporalCodingRanges) {
            boolean tValidate = temporalCodingRange.isValidated();
            for (short shard = 0; shard < shardNum; shard++) {
              ByteArray byteArray1 =
                      toRowKeyRangeBoundary(
                              shard,
                              new ByteArray(Bytes.toBytes(xzCode)),
                              temporalCodingRange.getLower(),
                              false);
              ByteArray byteArray2 =
                      toRowKeyRangeBoundary(
                              shard,
                              new ByteArray(Bytes.toBytes(xzCode)),
                              temporalCodingRange.getUpper(),
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
      List<RowKeyRange> result = new ArrayList<>();
      SpatialQueryCondition spatialQueryCondition =
              spatialTemporalQueryCondition.getSpatialQueryCondition();
      TemporalQueryCondition temporalQueryCondition =
              spatialTemporalQueryCondition.getTemporalQueryCondition();
      // 四重循环，所有可能的时间编码都应单独取值
      for (CodingRange spatialCodingRange : xz2Coding.ranges(spatialQueryCondition)) {
        long lowerXZ2Code = Bytes.toLong(spatialCodingRange.getLower().getBytes());
        long upperXZ2Code = Bytes.toLong(spatialCodingRange.getUpper().getBytes());
        boolean sValidate = spatialCodingRange.isValidated();
        List<CodingRange> temporalCodingRanges = xztCoding.ranges(temporalQueryCondition);
        for (long xzCode = lowerXZ2Code; xzCode <= upperXZ2Code; xzCode++) {
          short shard =
                  (short)
                          Math.abs(
                                  (spatialCodingRange.byteSfcCode(xzCode).hashCode()
                                          / DEFAULT_range_NUM
                                          % shardNum));
          for (CodingRange temporalCodingRange : temporalCodingRanges) {
            boolean tValidate = temporalCodingRange.isValidated();
            ByteArray byteArray1 =
                    toRowKeyRangeBoundary(
                            shard,
                            new ByteArray(Bytes.toBytes(xzCode)),
                            temporalCodingRange.getLower(),
                            false);
            ByteArray byteArray2 =
                    toRowKeyRangeBoundary(
                            shard,
                            new ByteArray(Bytes.toBytes(xzCode)),
                            temporalCodingRange.getUpper(),
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
  public String parsePhysicalIndex2String(ByteArray physicalIndex) {
    return "Row key index: {"
        + "shardNum="
        + getShardNum(physicalIndex)
        + ", xz2="
        + extractSpatialCode(physicalIndex)
        + ", bin = "
        + getTimeBin(physicalIndex)
        + ", timeCoding = "
        + getTimeElementCode(physicalIndex)
        + ", oidAndTid="
        + getObjectID(physicalIndex)
        + "-"
        + getTrajectoryID(physicalIndex)
        + '}';
  }

  public SpatialCoding getSpatialCoding() {
    return xz2Coding;
  }

  public ByteArray extractSpatialCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    byte[] bytes = new byte[Long.BYTES];
    buffer.get(bytes);
    return new ByteArray(bytes);
  }

  public TimeCoding getTimeCoding() {
    return xztCoding;
  }

  public TimeBin getTimeBin(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    return xztCoding.getTimeBin(buffer.getLong());
  }

  public long getTimeElementCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    buffer.getLong();
    return xztCoding.getElementCode(buffer.getLong());
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
    byte[] oidBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(oidBytes);
    return new String(oidBytes, StandardCharsets.UTF_8);
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
