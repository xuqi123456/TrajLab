package whu.edu.cn.trajlab.db.index.time;

import whu.edu.cn.trajlab.db.coding.coding.CodingRange;
import whu.edu.cn.trajlab.db.coding.coding.TimeCoding;
import whu.edu.cn.trajlab.db.coding.coding.XZTCoding;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.datatypes.TimeBin;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.IndexStrategy;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.constant.CodingConstants;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;


/**
 * @author xuqi
 * @date 2023/12/01
 */
public class IDTIndexStrategy extends IndexStrategy {

  private static Logger LOGGER = LoggerFactory.getLogger(IDTIndexStrategy.class);

  private final XZTCoding timeCoding;

  /** 作为行键时的字节数 */
  private static final int PHYSICAL_KEY_BYTE_LEN =
      Short.BYTES + CodingConstants.MAX_OID_LENGTH + XZTCoding.BYTES_NUM + CodingConstants.MAX_TID_LENGTH;

  private static final int LOGICAL_KEY_BYTE_LEN =
      PHYSICAL_KEY_BYTE_LEN - Short.BYTES - CodingConstants.MAX_TID_LENGTH;
  private static final int SCAN_RANGE_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - CodingConstants.MAX_TID_LENGTH;

  public IDTIndexStrategy(XZTCoding timeCoding) {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = timeCoding;
  }

  public IDTIndexStrategy() {
    indexType = IndexType.OBJECT_ID_T;
    this.timeCoding = new XZTCoding();
  }

  @Override
  /** ID-T索引中，shard由object id的hashcode生成，在负载均衡的同时，同ID数据保持本地性 */
  public ByteArray index(Trajectory trajectory) {
    try {
      ByteArray logicalIndex = logicalIndex(trajectory);
      short shard = getShard(trajectory.getObjectID());
      ByteBuffer buffer = ByteBuffer.allocate(logicalIndex.getBytes().length + Short.BYTES);
      buffer.put(Bytes.toBytes(shard));
      buffer.put(logicalIndex.getBytes());
      return new ByteArray(buffer.array());
    } catch (Exception e) {
      LOGGER.error("Failed to get index of: " + trajectory.toString());
      throw new IllegalArgumentException();
    }
  }

  @Override
  protected ByteArray logicalIndex(Trajectory trajectory) {
    TimeLine timeLine =
        new TimeLine(
            trajectory.getTrajectoryFeatures().getStartTime(),
            trajectory.getTrajectoryFeatures().getEndTime());
    long timeIndex = timeCoding.getIndex(timeLine);
    byte[] bytesEnd = trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8);
    ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN + bytesEnd.length);
    byteBuffer.put(getObjectIDBytes(trajectory));
    byteBuffer.putLong(timeIndex);
    byteBuffer.put(getTrajectoryIDBytes(trajectory));
    return new ByteArray(byteBuffer);
  }

  @Override
  public List<RowKeyRange> getScanRanges(AbstractQueryCondition queryCondition) {
    throw new IllegalArgumentException();
  }

  /** 先去掉头部的shard和OID信息 */
  public TimeLine getTimeLineRange(ByteArray byteArray) {
    return timeCoding.getXZTElementTimeLine(getTimeCode(byteArray));
  }

  /**
   * @param abstractQueryCondition Time query range
   * @param oId Trajectory ID
   * @return List of XZT index ranges corresponding to the query range.
   */
  @Override
  public List<RowKeyRange> getScanRanges(
      AbstractQueryCondition abstractQueryCondition, String oId) {
    if (abstractQueryCondition instanceof TemporalQueryCondition) {
      TemporalQueryCondition temporalQueryCondition =
          (TemporalQueryCondition) abstractQueryCondition;
      List<RowKeyRange> result = new ArrayList<>();
      List<CodingRange> codingRanges = timeCoding.ranges(temporalQueryCondition);
      for (CodingRange codingRange : codingRanges) {
        short shard = getShard(oId);
        ByteArray byteArray1 = toRowKeyRangeBoundary(shard, codingRange.getLower(), oId, false);
        ByteArray byteArray2 = toRowKeyRangeBoundary(shard, codingRange.getUpper(), oId, true);
        result.add(new RowKeyRange(byteArray1, byteArray2, codingRange.isValidated()));
      }
      return result;
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public String parsePhysicalIndex2String(ByteArray byteArray) {
    return "Row key index: {"
        + "shardNum = "
        + getShardNum(byteArray)
        + ", OID = "
        + getObjectID(byteArray)
        + ", XZT = "
        + getTimeCode(byteArray)
        + '}';
  }

  public TimeCoding getTimeCoding() {
    return timeCoding;
  }

  public long getTimeElementCode(ByteArray byteArray) {
    return timeCoding.getElementCode(getTimeCode(byteArray));
  }

  public TimeBin getTimeBin(ByteArray byteArray) {
    return timeCoding.getTimeBin(getTimeCode(byteArray));
  }

  public long getTimeCode(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    buffer.getShort();
    for (int i = 0; i < CodingConstants.MAX_OID_LENGTH; i++) {
      buffer.get();
    }
    return buffer.getLong();
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
    byte[] oidBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(oidBytes);
    return new String(oidBytes, StandardCharsets.UTF_8);
  }

  @Override
  public String getTrajectoryID(ByteArray byteArray) {
    ByteBuffer buffer = byteArray.toByteBuffer();
    ((Buffer) buffer).flip();
    // shard
    buffer.getShort();
    // OID
    byte[] oidBytes = new byte[CodingConstants.MAX_OID_LENGTH];
    buffer.get(oidBytes);
    // time code
    buffer.getLong();
    // TID
    int validTidLength = buffer.remaining();
    byte[] validTidBytes = new byte[validTidLength];
    buffer.get(validTidBytes);
    return new String(validTidBytes, StandardCharsets.UTF_8);
  }

  private ByteArray toRowKeyRangeBoundary(
      short shard, ByteArray timeBytes, String oId, Boolean end) {
    byte[] oidBytesPadding = getObjectIDBytes(oId);
    ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
    byteBuffer.putShort(shard);
    byteBuffer.put(oidBytesPadding);
    if (end) {
      byteBuffer.putLong(Bytes.toLong(timeBytes.getBytes()) + 1);
    } else {
      byteBuffer.put(timeBytes.getBytes());
    }
    return new ByteArray(byteBuffer);
  }

  private short getShard(String oid) {
    return (short) Math.abs(oid.hashCode() % shardNum);
  }
}
