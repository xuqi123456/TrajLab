package whu.edu.cn.trajlab.db.index;

import whu.edu.cn.trajlab.db.datatypes.ByteArray;

import java.util.Objects;

/**
 * A range represents some continuous row keys, and the range will be constructed into an HBase scan object.
 * RowKeyRange对象的startKey为Included，而endKey为Excluded（endKey为大于startKey的最小不符合条件的key，不能included）
 * @author xuqi
 * @date 2023/12/01
 */
public class RowKeyRange {
    ByteArray startKey;
    ByteArray endKey;
    boolean validate;
    short shardKey;

    public RowKeyRange(ByteArray startKey, ByteArray endKey) {
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public RowKeyRange(ByteArray startKey, ByteArray endKey, boolean validate) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.validate = validate;
    }

    public RowKeyRange(ByteArray startKey, ByteArray endKey, boolean validate, short shardKey) {
        this.startKey = startKey;
        this.endKey = endKey;
        this.validate = validate;
        this.shardKey = shardKey;
    }

    public ByteArray getStartKey() {
        return startKey;
    }

    public ByteArray getEndKey() {
        return endKey;
    }

    public boolean isValidate() {
        return validate;
    }

    public short getShardKey() {
        return shardKey;
    }

    @Override
    public String toString() {
        return "RowKeyRange{" +
                "startKey=" + startKey +
                ", endKey=" + endKey +
                ", validate=" + validate +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RowKeyRange that = (RowKeyRange) o;
        return shardKey == that.shardKey && Objects.equals(startKey, that.startKey) && Objects.equals(endKey, that.endKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(startKey, endKey, shardKey);
    }
}
