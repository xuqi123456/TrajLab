package whu.edu.cn.trajlab.db.database.mapper;

import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.database.util.TrajectorySerdeUtils;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.constant.DBConstants;
import whu.edu.cn.trajlab.db.database.util.BulkLoadDriverUtils;

import java.io.IOException;


/**
 * @author xuqi
 * @date 2023/12/06
 */
public class MainToSecondaryMapper extends TableMapper<ImmutableBytesWritable, Put> {

  private static IndexTable secondaryTable;

  @Override
  protected void setup(
      Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Put>.Context context)
      throws IOException, InterruptedException {
    super.setup(context);
    secondaryTable = BulkLoadDriverUtils.getIndexTable(context.getConfiguration());
  }

  @SuppressWarnings("rawtypes")
  public static void initJob(String table, Scan scan, Class<? extends TableMapper> mapper, Job job)
      throws IOException {
    TableMapReduceUtil.initTableMapperJob(
        table, scan, mapper, ImmutableBytesWritable.class, Result.class, job);
  }

  @Override
  protected void map(ImmutableBytesWritable key, Result coreIndexRow, Context context)
      throws IOException, InterruptedException {
    byte[] coreIndexRowKey = key.get();
    Trajectory t = TrajectorySerdeUtils.getTrajectoryFromResult(coreIndexRow);
    Put p =
        TrajectorySerdeUtils.getPutForSecondaryIndex(
            secondaryTable.getIndexMeta(),
            t,
            coreIndexRowKey,
            context.getConfiguration().getBoolean(DBConstants.ENABLE_SIMPLE_SECONDARY_INDEX, false));
    context.write(new ImmutableBytesWritable(p.getRow()), p);
  }
}
