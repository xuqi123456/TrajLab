package whu.edu.cn.trajlab.query.query.basic;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.constant.DBConstants;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.mapper.TrajectoryDataMapper;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import org.apache.spark.api.java.JavaRDD;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xuqi
 * @date 2023/12/11
 */
public class IDQuery extends AbstractQuery {
    private static final Database instance;

    static {
        try {
            instance = Database.getInstance();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public IDQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
        super(dataSet, abstractQueryCondition);
    }

    public IDQuery(IndexTable indexTable, AbstractQueryCondition abstractQueryCondition)
            throws IOException {
        super(indexTable, abstractQueryCondition);
    }

    @Override
    public JavaRDD<Trajectory> getRDDQuery(SparkSession sc) throws IOException {
        List<RowKeyRange> indexRanges = getIndexRanges();
        JavaSparkContext context = SparkUtils.getJavaSparkContext(sc);
        JavaRDD<RowKeyRange> rowKeyRangeJavaRDD = context.parallelize(indexRanges);

        return rowKeyRangeJavaRDD.flatMap(t -> {
            ArrayList<RowKeyRange> list = new ArrayList<>();
            list.add(t);
            return executeQuery(list).iterator();
        });
    }

    public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
        ArrayList<Result> list = new ArrayList<>();
        ArrayList<Trajectory> trajectories = new ArrayList<>();
        if (!targetIndexTable.getIndexMeta().isMainIndex()) {
            Scan scan = buildScan(rowKeyRanges.get(0));
            ResultScanner results = instance.getScan(targetIndexTable.getTable(), scan);
            for (Result result : results) {
                list.add(instance.getMainIndexedResult(result, targetIndexTable));
            }
        } else {
            Scan scan = buildCoreScan(rowKeyRanges.get(0));
            ResultScanner results = instance.getScan(targetIndexTable.getTable(), scan);
            for (Result result : results) {
                list.add(result);
            }
        }
        for (Result result : list) {
            trajectories.add(TrajectoryDataMapper.mapHBaseResultToTrajectory(result));
        }
        return trajectories;
    }

    protected Scan buildScan(RowKeyRange rowKeyRange) {
        Scan scan = new Scan();
        scan.withStartRow(rowKeyRange.getStartKey().getBytes(), true);
        scan.withStopRow(rowKeyRange.getEndKey().getBytes(), false);
        scan.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER);
        scan.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER);
        scan.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.PTR_QUALIFIER);
        return scan;
    }

    protected Scan buildCoreScan(RowKeyRange rowKeyRange) {
        Scan scan = new Scan();
        scan.withStartRow(rowKeyRange.getStartKey().getBytes(), true);
        scan.withStopRow(rowKeyRange.getEndKey().getBytes(), false);
        scan.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.OBJECT_ID_QUALIFIER);
        scan.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.TRAJECTORY_ID_QUALIFIER);
        scan.addColumn(DBConstants.COLUMN_FAMILY, DBConstants.TRAJ_POINTS_QUALIFIER);
        return scan;
    }

    @Override
    public IndexMeta findBestIndex() {
        Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
        // find a ID index
        List<IndexMeta> indexList = null;
        if (map.containsKey(IndexType.ID)) {
            indexList = map.get(IndexType.ID);
        }
        if (indexList != null) {
            return IndexMeta.getBestIndexMeta(indexList);
        }
        // no spatial index so we will do a full table scan, we select a main index.
        return dataSet.getDataSetMeta().getCoreIndexMeta();
    }

    @Override
    public String getQueryInfo() {
        return abstractQueryCondition.getConditionInfo();
    }
}
