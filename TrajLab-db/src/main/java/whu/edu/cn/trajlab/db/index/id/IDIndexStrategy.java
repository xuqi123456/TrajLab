package whu.edu.cn.trajlab.db.index.id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.constant.CodingConstants;
import whu.edu.cn.trajlab.db.datatypes.ByteArray;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.IndexStrategy;
import whu.edu.cn.trajlab.db.index.RowKeyRange;

import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/01/20
 */
public class IDIndexStrategy extends IndexStrategy {
    private static Logger LOGGER = LoggerFactory.getLogger(IDIndexStrategy.class);
    private static final int PHYSICAL_KEY_BYTE_LEN =
            Short.BYTES + CodingConstants.MAX_OID_LENGTH + CodingConstants.MAX_TID_LENGTH;

    private static final int LOGICAL_KEY_BYTE_LEN =
            PHYSICAL_KEY_BYTE_LEN - Short.BYTES - CodingConstants.MAX_TID_LENGTH;
    private static final int PARTITION_KEY_BYTE_LEN = CodingConstants.MAX_OID_LENGTH;
    private static final int SCAN_RANGE_BYTE_LEN = PHYSICAL_KEY_BYTE_LEN - CodingConstants.MAX_TID_LENGTH;

    public IDIndexStrategy() {
        indexType = IndexType.ID;
    }

    @Override
    protected ByteArray logicalIndex(Trajectory trajectory) {
        byte[] bytesEnd = trajectory.getTrajectoryID().getBytes(StandardCharsets.UTF_8);
        ByteBuffer byteBuffer = ByteBuffer.allocate(LOGICAL_KEY_BYTE_LEN + bytesEnd.length);
        byteBuffer.put(getObjectIDBytes(trajectory));
        byteBuffer.put(getTrajectoryIDBytes(trajectory));
        return new ByteArray(byteBuffer);
    }

    @Override
    protected ByteArray partitionIndex(Trajectory trajectory) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(PARTITION_KEY_BYTE_LEN);
        byteBuffer.put(getObjectIDBytes(trajectory));
        return new ByteArray(byteBuffer);
    }

    @Override
    public List<RowKeyRange> getScanRanges(AbstractQueryCondition abstractQueryCondition) {
        if (abstractQueryCondition instanceof IDQueryCondition) {
            IDQueryCondition idQueryCondition =
                    (IDQueryCondition) abstractQueryCondition;
            List<RowKeyRange> result = new ArrayList<>();
            String moid = idQueryCondition.getMoid();
            short shard = getShardByOid(moid);
            ByteArray byteArray1 = toRowKeyRangeBoundary(shard, moid);
            ByteArray byteArray2 = toRowKeyRangeBoundary(shard, moid);
            result.add(new RowKeyRange(byteArray1, byteArray2));
            return result;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public List<RowKeyRange> getPartitionScanRanges(AbstractQueryCondition queryCondition) {
        return getScanRanges(queryCondition);
    }

    private ByteArray toRowKeyRangeBoundary(
            short shard, String oId) {
        byte[] objectIDBytes = getObjectIDBytes(oId);
        ByteBuffer byteBuffer = ByteBuffer.allocate(SCAN_RANGE_BYTE_LEN);
        byteBuffer.putShort(shard);
        byteBuffer.put(objectIDBytes);
        return new ByteArray(byteBuffer);
    }

    @Override
    public String parsePhysicalIndex2String(ByteArray byteArray) {
        return "Row key index: {"
                + "shardNum = "
                + getShardNum(byteArray)
                + ", OID = "
                + getObjectID(byteArray)
                + ", TID = "
                + getTrajectoryID(byteArray)
                + '}';
    }

    @Override
    public String parseScanIndex2String(ByteArray byteArray) {
        return "Row key index: {"
                + "shardNum = "
                + getShardNum(byteArray)
                + ", OID = "
                + getObjectID(byteArray)
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
        // TID
        int validTidLength = buffer.remaining();
        byte[] validTidBytes = new byte[validTidLength];
        buffer.get(validTidBytes);
        return new String(validTidBytes, StandardCharsets.UTF_8);
    }
}
