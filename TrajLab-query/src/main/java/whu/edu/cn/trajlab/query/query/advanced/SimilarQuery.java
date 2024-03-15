package whu.edu.cn.trajlab.query.query.advanced;

import com.google.protobuf.ByteString;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import whu.edu.cn.trajlab.base.point.TrajPoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.util.GeoUtils;
import whu.edu.cn.trajlab.base.util.SparkUtils;
import whu.edu.cn.trajlab.db.condition.*;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.query.coprocessor.STCoprocessorQuery;
import whu.edu.cn.trajlab.query.coprocessor.autogenerated.QueryCondition;
import whu.edu.cn.trajlab.query.query.basic.AbstractQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static whu.edu.cn.trajlab.query.query.basic.TemporalQuery.buildProtoTemporalWindow;

/**
 * @author xuqi
 * @date 2024/01/29
 */
public class SimilarQuery extends AbstractQuery {
    private static Logger logger = LoggerFactory.getLogger(SimilarQuery.class);

    public SimilarQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
        super(dataSet, abstractQueryCondition);
    }

    public SimilarQuery(IndexTable targetIndexTable, AbstractQueryCondition abstractQueryCondition)
            throws IOException {
        super(targetIndexTable, abstractQueryCondition);
    }

    private boolean hasTimeConstrain() {
        if (abstractQueryCondition instanceof SimilarQueryCondition) {
            SimilarQueryCondition similarQueryCondition = (SimilarQueryCondition) abstractQueryCondition;
            return similarQueryCondition.getTemporalQueryCondition() != null;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public List<Trajectory> executeQuery() throws IOException {
        setupTargetIndexTable();

        SimilarQueryCondition sqc = (SimilarQueryCondition) abstractQueryCondition;
        Trajectory centralTrajectory = sqc.getCentralTrajectory();
        // 轨迹起止点作为过滤条件
        TrajPoint startPoint = centralTrajectory.getTrajectoryFeatures().getStartPoint();
        TrajPoint endPoint = centralTrajectory.getTrajectoryFeatures().getEndPoint();
        Geometry buffer1 = startPoint.buffer(GeoUtils.getDegreeFromKm(sqc.getSimDisThreshold()));
        Geometry buffer2 = endPoint.buffer(GeoUtils.getDegreeFromKm(sqc.getSimDisThreshold()));
        AbstractQueryCondition qc1 = generateSTQueryCondition(buffer1);
        AbstractQueryCondition qc2 = generateSTQueryCondition(buffer2);
        List<RowKeyRange> splitRanges = getSplitRanges(qc1, qc2);


        List<Trajectory> trajectories = executeQuery(splitRanges);
        trajectories.remove(centralTrajectory);
        logger.info("Start SimilarQuery at {} SimDisThreshold", sqc.getSimDisThreshold());
        return trajectories;
    }

    @Override
    public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
        setupTargetIndexTable();
        List<QueryCondition.Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
        SimilarQueryCondition similarQueryCondition = (SimilarQueryCondition) abstractQueryCondition;

        QueryCondition.TemporalQueryWindow temporalQueryWindow =
                buildProtoTemporalWindow(similarQueryCondition.getTemporalQueryCondition());
        QueryCondition.QueryRequest simQueryRequest;
        if (!hasTimeConstrain()) {
            simQueryRequest =
                    QueryCondition.QueryRequest.newBuilder()
                            .addAllRange(ranges)
                            .setSim(
                                    QueryCondition.SimilarQueryRequest.newBuilder()
                                            .setDistance(
                                                    GeoUtils.getDegreeFromKm(similarQueryCondition.getSimDisThreshold()))
                                            .setTrajectory(ByteString.copyFrom(similarQueryCondition.getTrajectoryBytes()))
                                            .build())
                            .setQueryOperation(QueryCondition.QueryMethod.SIMILAR)
                            .build();
        } else {
            simQueryRequest =
                    QueryCondition.QueryRequest.newBuilder()
                            .addAllRange(ranges)
                            .setSim(
                                    QueryCondition.SimilarQueryRequest.newBuilder()
                                            .setDistance(
                                                    GeoUtils.getDegreeFromKm(similarQueryCondition.getSimDisThreshold()))
                                            .setTrajectory(ByteString.copyFrom(similarQueryCondition.getTrajectoryBytes()))
                                            .setTemporalQueryWindow(
                                                    QueryCondition.TemporalQueryWindow.newBuilder()
                                                            .setStartMs(temporalQueryWindow.getStartMs())
                                                            .setEndMs(temporalQueryWindow.getEndMs())
                                                            .build()
                                            )
                                            .build())
                            .setQueryOperation(QueryCondition.QueryMethod.SIMILAR)
                            .build();
        }

        return STCoprocessorQuery.executeQuery(targetIndexTable, simQueryRequest);
    }

    @Override
    public JavaRDD<Trajectory> getRDDQuery(SparkSession ss) throws IOException {
        setupTargetIndexTable();
        JavaSparkContext context = SparkUtils.getJavaSparkContext(ss);
        SimilarQueryCondition sqc = (SimilarQueryCondition) abstractQueryCondition;
        Trajectory centralTrajectory = sqc.getCentralTrajectory();
        // 轨迹起止点作为过滤条件
        TrajPoint startPoint = centralTrajectory.getTrajectoryFeatures().getStartPoint();
        TrajPoint endPoint = centralTrajectory.getTrajectoryFeatures().getEndPoint();
        Geometry buffer1 = startPoint.buffer(GeoUtils.getDegreeFromKm(sqc.getSimDisThreshold()));
        Geometry buffer2 = endPoint.buffer(GeoUtils.getDegreeFromKm(sqc.getSimDisThreshold()));
        AbstractQueryCondition qc1 = generateSTQueryCondition(buffer1);
        AbstractQueryCondition qc2 = generateSTQueryCondition(buffer2);
        List<RowKeyRange> splitRanges = getSplitRanges(qc1, qc2);
        JavaRDD<RowKeyRange> rowKeyRangeJavaRDD = context.parallelize(splitRanges);

        JavaRDD<Trajectory> trajRDD =
                rowKeyRangeJavaRDD
                        .groupBy(RowKeyRange::getShardKey)
                        .flatMap(
                                iteratorPair -> {
                                    // 对每个分区中的元素进行转换操作
                                    List<RowKeyRange> result = new ArrayList<>();
                                    for (RowKeyRange rowKeyRange : iteratorPair._2) {
                                        result.add(rowKeyRange);
                                    }
                                    return executeQuery(result).iterator();
                                });
        logger.info("Start SimilarQuery at {} SimDisThreshold", sqc.getSimDisThreshold());

        return trajRDD.filter(t -> !t.equals(centralTrajectory));
    }

    public AbstractQueryCondition generateSTQueryCondition(Geometry geom) throws IOException {
        SimilarQueryCondition sqc = (SimilarQueryCondition) abstractQueryCondition;
        SpatialQueryCondition spatialQc =
                new SpatialQueryCondition(geom, SpatialQueryCondition.SpatialQueryType.INTERSECT);
        if (hasTimeConstrain()) {
            TemporalQueryCondition tqc = sqc.getTemporalQueryCondition();
            return new SpatialTemporalQueryCondition(spatialQc, tqc);
        } else {
            return spatialQc;
        }
    }

    public List<RowKeyRange> getSplitRanges(
            AbstractQueryCondition abQc1, AbstractQueryCondition abQc2) throws IOException {
        setupTargetIndexTable();
        List<RowKeyRange> partitionScanRanges1 =
                targetIndexTable.getIndexMeta().getIndexStrategy().getPartitionScanRanges(abQc1);
        List<RowKeyRange> partitionScanRanges2 =
                targetIndexTable.getIndexMeta().getIndexStrategy().getPartitionScanRanges(abQc2);
        partitionScanRanges1.retainAll(partitionScanRanges2);

        return partitionScanRanges1;
    }

    @Override
    public IndexMeta findBestIndex() {
        Map<IndexType, List<IndexMeta>> map = dataSet.getDataSetMeta().getAvailableIndexes();
        // case 1: 无时间约束，找XZ2索引，或XZ2T索引
        if (!hasTimeConstrain()) {
            if (map.containsKey(IndexType.XZ2)) {
                return IndexMeta.getBestIndexMeta(map.get(IndexType.XZ2));
            }
        }
        // case 2: 有时间约束，找TXZ2索引
        else {
            if (map.containsKey(IndexType.XZ2T)) {
                return IndexMeta.getBestIndexMeta(map.get(IndexType.XZ2T));
            }
            if (map.containsKey(IndexType.TXZ2)) {
                return IndexMeta.getBestIndexMeta(map.get(IndexType.TXZ2));
            }
        }
        return null;
    }

    @Override
    public String getQueryInfo() {
        return abstractQueryCondition.getConditionInfo();
    }
}
