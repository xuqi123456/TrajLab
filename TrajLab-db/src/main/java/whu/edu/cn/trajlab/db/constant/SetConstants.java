package whu.edu.cn.trajlab.db.constant;

import org.apache.hadoop.hbase.util.Bytes;


/**
 * @author xuqi
 * @date 2023/12/05
 */
public class SetConstants {
    public static final String start_time = "1970-01-01 00:00:00";
    public static final int srid = 4326;
    public static final byte[] DATA_COUNT = Bytes.toBytes("data_count");
    public static final byte[] DATA_MBR = Bytes.toBytes("data_mbr");
    public static final byte[] DATA_START_TIME = Bytes.toBytes("data_start_time");
    public static final byte[] DATA_END_TIME = Bytes.toBytes("data_end_time");
}
