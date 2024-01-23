package whu.edu.cn.trajlab.db.index.time;

import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.coding.coding.CodingRange;
import whu.edu.cn.trajlab.db.coding.coding.XZTCoding;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.constant.CodingConstants;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.IndexStrategy;
import whu.edu.cn.trajlab.db.index.RowKeyRange;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.IndexConstants.DEFAULT_range_NUM;

/**
 * @author xuqi
 * @date 2024/01/20
 */
public class TemporalIndexStrategy extends IndexStrategy {
  private static Logger LOGGER = LoggerFactory.getLogger(TemporalIndexStrategy.class);

  private XZTCoding timeCoding;

  /** 作为行键时的字节数 */
  private static final int PHYSICAL_KEY_BYTE_LEN =
      Short.BYTES
          + XZTCoding.BYTES_NUM
          + CodingConstants.MAX_OID_LENGTH
          + CodingConstants.MAX_TID_LENGTH;

  private static final int LOGICAL_KEY_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - Short.BYTES - CodingConstants.MAX_TID_LENGTH;
  private static final int PARTITION_KEY_BYTE_LEN = XZTCoding.BYTES_NUM;
  private static final int SCAN_RANGE_BYTE_LEN =
      PHYSICAL_KEY_BYTE_LEN - CodingConstants.MAX_TID_LENGTH - CodingConstants.MAX_OID_LENGTH;

  public TemporalIndexStrategy() {
    indexType = IndexType.Temporal;
    this.timeCoding = new XZTCoding();
  }

  public TemporalIndexStrategy(XZTCoding timeCoding) {
    indexType = IndexType.Temporal;
    this.timeCoding = timeCoding;
  }

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    TimeLine timeLine =
        new TimeLine(
            trajectory.getTrajectoryFeatures().getStartTime(),
            trajectory.getTrajectoryFeatures().getEndTime());
    long timeIndex = timeCoding.getIndex(timeLine);
    byte[] bytesEnd = trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer =
        ByteBuffer.allocate(
            LOGICAL_KEY_BYTE_LEN + bytesEnd.length);
    byteBuffer.putLong(timeIndex);
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
    long timeIndex = timeCoding.getIndex(timeLine);
    ByteBuffer byteBuffer = ByteBuffer.allocate(PARTITION_KEY_BYTE_LEN);
    byteBuffer.putLong(timeIndex);
    return new ByteArray(byteBuffer);
  }

  @Override
  public List<RowKeyRange> getScanRanges(AbstractQueryCondition abstractQueryCondition) {
    if (abstractQueryCondition instanceof TemporalQueryCondition) {
      TemporalQueryCondition temporalQueryCondition =
          (TemporalQueryCondition) abstractQueryCondition;
      List<RowKeyRange> result = new ArrayList<>();
      List<CodingRange> codingRanges =
          timeCoding.ranges(temporalQueryCondition);
      for (CodingRange codingRange : codingRanges) {
        for(short shard = 0; shard < shardNum; shard++) {
          ByteArray byteArray1 = toRowKeyRangeBoundary(shard, codingRange.getLower(), false);
          ByteArray byteArray2 = toRowKeyRangeBoundary(shard, codingRange.getUpper(), true);
          result.add(new RowKeyRange(byteArray1, byteArray2, codingRange.isValidated()));
        }
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public List<RowKeyRange> getPartitionScanRanges(AbstractQueryCondition queryCondition) {
    if (queryCondition instanceof TemporalQueryCondition) {
      TemporalQueryCondition temporalQueryCondition = (TemporalQueryCondition) queryCondition;
      List<RowKeyRange> result = new ArrayList<>();
      // 1. xz2 coding
      List<CodingRange> codingRanges = timeCoding.rangesWithPartition(temporalQueryCondition);
      // 2. concat shard index
      for (CodingRange xztCoding : codingRanges) {
        short shard = (short) Math.abs((xztCoding.getLower().hashCode() / DEFAULT_range_NUM % shardNum));
        result.add(
                new RowKeyRange(
                        toRowKeyRangeBoundary(shard, xztCoding.getLower(), false),
                        toRowKeyRangeBoundary(shard, xztCoding.getUpper(), true),
                        xztCoding.isValidated(), shard));
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  private ByteArray toRowKeyRangeBoundary(
          short shard, ByteArray timeBytes, Boolean end) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
    byteBuffer.putShort(shard);
    if (end) {
      byteBuffer.putLong(Bytes.toLong(timeBytes.getBytes()) + 1);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {"
            + "shardNum = "
            + getShardNum(byteArray)
            + ", XZT = "
            + getTimeCode(byteArray)
            + ", oidAndTid="
            + getObjectID(byteArray)
            + "-"
            + getTrajectoryID(byteArray)
            + '}';
  }

  @Override
  public String parseScanIndex2String(ByteArray byteArray) {
    return "Row key index: {"
            + "shardNum = "
            + getShardNum(byteArray)
            + ", XZT = "
            + getTimeCode(byteArray)
            + '}';
  }

  @Override
  public short getShardNum(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    ((Buffer) buffer).flip();
    return buffer.getShort();
  }

  @Override
  public String getObjectID(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort(); // shard
    buffer.getLong();
    byte[] oidBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(oidBytes);
    return new String(oidBytes, StandardCharsets.UTF_8);
  }

  @Override
  public String getTrajectoryID(ByteArray physicalIndex) {
    ByteBuffer buffer = physicalIndex.toByteBuffer();
    ((Buffer) buffer).flip();
    // shard
    buffer.getShort();
    // time code
    buffer.getLong();
    // OID
    byte[] oidBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(oidBytes);
    // TID
    int validTidLength = buffer.remaining();
    byte[] validTidBytes = new byte[validTidLength];
    buffer.get(validTidBytes);
    return new String(validTidBytes, StandardCharsets.UTF_8);
  }
  public long getTimeCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    return buffer.getLong();
  }
}
