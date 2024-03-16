package whu.edu.cn.trajlab.query.query.advanced;

import com.google.protobuf.ByteString;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKTWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import whu.edu.cn.trajlab.base.point.BasePoint;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.base.trajectory.TrajectoryWithDistance;
import whu.edu.cn.trajlab.base.util.GeoUtils;
import whu.edu.cn.trajlab.base.util.SerializerUtils;
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
import java.util.*;

import static whu.edu.cn.trajlab.query.constants.QueryConstants.BASIC_BUFFER_DISTANCE;
import static whu.edu.cn.trajlab.query.constants.QueryConstants.MAX_QUERY_NOTHING_TIME;
import static whu.edu.cn.trajlab.query.query.basic.TemporalQuery.buildProtoTemporalWindow;

/**
 * @author xuqi
 * @date 2024/01/24
 */
public class KNNQuery extends AbstractQuery {
    int stage = 0;
    double curSearchDist = BASIC_BUFFER_DISTANCE;
    double maxDistance = Double.MAX_VALUE;

    private static Logger logger = LoggerFactory.getLogger(KNNQuery.class);

    public KNNQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
        super(dataSet, abstractQueryCondition);
    }

    public KNNQuery(
            IndexTable targetIndexTable, AbstractQueryCondition abstractQueryCondition)
            throws IOException {
        super(targetIndexTable, abstractQueryCondition);
    }

    private boolean hasTimeConstrain() {
        if (abstractQueryCondition instanceof KNNQueryCondition) {
            KNNQueryCondition knnQueryCondition = (KNNQueryCondition) abstractQueryCondition;
            return knnQueryCondition.getTemporalQueryCondition() != null;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * 大顶堆求k个最小值
     */
    private void addToHeap(
            List<Trajectory> trajList, PriorityQueue<TrajectoryWithDistance> pq, KNNQueryCondition kqc) {
        switch (kqc.getKnnQueryType()) {
            case Point: {
                for (Trajectory t : trajList) {
                    TrajectoryWithDistance kt = new TrajectoryWithDistance(t, kqc.getCentralPoint());
                    if(pq.contains(kt)) continue;
                    if (pq.size() < kqc.getK()) {
                        pq.offer(kt);
                    } else if (pq.peek().getDistance() > kt.getDistance()) {
                        pq.poll();
                        pq.offer(kt);
                    }
                }
                break;
            }
            case Trajectory: {
                for (Trajectory t : trajList) {
                    TrajectoryWithDistance kt = new TrajectoryWithDistance(t, kqc.getCentralTrajectory());
                    if (pq.size() < kqc.getK()) {
                        pq.offer(kt);
                    } else if (pq.peek().getDistance() > kt.getDistance()) {
                        pq.poll();
                        pq.offer(kt);
                    }
                }
                break;
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    public List<Trajectory> heapToResultList(PriorityQueue<TrajectoryWithDistance> pq) {
        List<Trajectory> result = new LinkedList<>();
        while (!pq.isEmpty()) {
            result.add(pq.poll().getTrajectory());
        }
        return result;
    }

    @Override
    public List<Trajectory> executeQuery() throws IOException {
        setupTargetIndexTable();
        KNNQueryCondition kqc = (KNNQueryCondition) abstractQueryCondition;
        int k = kqc.getK();
        int resultSearch = 0;
        int searchNothing = 0;
        PriorityQueue<TrajectoryWithDistance> pq =
                new PriorityQueue<>(
                        k,
                        (o1, o2) -> {
                            double dist1 = o1.getDistance();
                            double dist2 = o2.getDistance();
                            return Double.compare(dist2, dist1);
                        });
        HashSet<RowKeyRange> set = new HashSet<>();
        switch (kqc.getKnnQueryType()) {
            case Point: {
                BasePoint centralPoint = kqc.getCentralPoint();
                while ((pq.size() < k && searchNothing < MAX_QUERY_NOTHING_TIME) || ( resultSearch != 0)) {
                    Geometry buffer = centralPoint.buffer(GeoUtils.getDegreeFromKm(curSearchDist));

                    AbstractQueryCondition stQc = generateSTQueryCondition(buffer);
                    Tuple2<List<RowKeyRange>, HashSet<RowKeyRange>> ranges = getSplitRanges(stQc, set);
                    List<RowKeyRange> splitRanges = ranges._1;
                    set = ranges._2;

                    List<Trajectory> collect = executeQuery(splitRanges);
                    resultSearch = collect.size();
                    if(resultSearch == 0) searchNothing++;
                    addToHeap(collect, pq, kqc);
                    if (!pq.isEmpty()) {
                        maxDistance = pq.peek().getDistance();
                    }
                    getSearchRadiusKM(resultSearch, kqc.getK(), curSearchDist);
                    stage++;
                    logger.info(
                            "Start search radius {} at stage {}, got {} numbers count.",
                            curSearchDist,
                            stage,
                            collect.size());
                }
                return heapToResultList(pq);
            }
            case Trajectory: {
                Trajectory centralTrajectory = kqc.getCentralTrajectory();
                while ((pq.size() < k && searchNothing < MAX_QUERY_NOTHING_TIME) || (resultSearch != 0)) {
                    Geometry buffer = centralTrajectory.buffer(GeoUtils.getDegreeFromKm(curSearchDist));
                    AbstractQueryCondition stQc = generateSTQueryCondition(buffer);
                    Tuple2<List<RowKeyRange>, HashSet<RowKeyRange>> ranges = getSplitRanges(stQc, set);
                    List<RowKeyRange> splitRanges = ranges._1;
                    set = ranges._2;
                    List<Trajectory> collect = executeQuery(splitRanges);

                    resultSearch = collect.size();
                    if(resultSearch == 0) searchNothing++;
                    addToHeap(collect, pq, kqc);
                    if (!pq.isEmpty()) {
                        maxDistance = pq.peek().getDistance();
                    }
                    getSearchRadiusKM(resultSearch, kqc.getK(), curSearchDist);
                    stage++;
                    logger.info(
                            "Start search radius {} at stage {}, got {} numbers count.",
                            curSearchDist,
                            stage,
                            collect.size());
                }
                return heapToResultList(pq);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }
    @Override
    public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
        setupTargetIndexTable();
        List<QueryCondition.Range> ranges = rowKeyRangeToProtoRange(rowKeyRanges);
        KNNQueryCondition knnQueryCondition = (KNNQueryCondition) abstractQueryCondition;
        QueryCondition.TemporalQueryWindow temporalQueryWindow =
                buildProtoTemporalWindow(knnQueryCondition.getTemporalQueryCondition());
        switch (knnQueryCondition.getKnnQueryType()) {
            case Point: {
                QueryCondition.QueryRequest knnQueryRequest;
                if (hasTimeConstrain()) {
                    knnQueryRequest =
                            QueryCondition.QueryRequest.newBuilder()
                                    .addAllRange(ranges)
                                    .setKnn(
                                            QueryCondition.KNNQueryRequest.newBuilder()
                                                    .setK(knnQueryCondition.getK())
                                                    .setPoint(ByteString.copyFrom(knnQueryCondition.getPointBytes()))
                                                    .setDistance(GeoUtils.getDegreeFromKm(maxDistance))
                                                    .setTemporalQueryWindow(
                                                            QueryCondition.TemporalQueryWindow.newBuilder()
                                                                    .setStartMs(temporalQueryWindow.getStartMs())
                                                                    .setEndMs(temporalQueryWindow.getEndMs())
                                                                    .build()
                                                    )
                                                    .build())
                                    .setQueryOperation(QueryCondition.QueryMethod.KNN)
                                    .build();
                } else {
                    knnQueryRequest =
                            QueryCondition.QueryRequest.newBuilder()
                                    .addAllRange(ranges)
                                    .setKnn(
                                            QueryCondition.KNNQueryRequest.newBuilder()
                                                    .setK(knnQueryCondition.getK())
                                                    .setPoint(ByteString.copyFrom(knnQueryCondition.getPointBytes()))
                                                    .setDistance(GeoUtils.getDegreeFromKm(maxDistance))
                                                    .build())
                                    .setQueryOperation(QueryCondition.QueryMethod.KNN)
                                    .build();
                }
                return STCoprocessorQuery.executeQuery(targetIndexTable, knnQueryRequest);
            }
            case Trajectory: {
                QueryCondition.QueryRequest knnQueryRequest;
                if (hasTimeConstrain()) {
                    knnQueryRequest =
                            QueryCondition.QueryRequest.newBuilder()
                                    .addAllRange(ranges)
                                    .setKnn(
                                            QueryCondition.KNNQueryRequest.newBuilder()
                                                    .setK(knnQueryCondition.getK())
                                                    .setTrajectory(
                                                            ByteString.copyFrom(knnQueryCondition.getTrajectoryBytes()))
                                                    .setDistance(GeoUtils.getDegreeFromKm(maxDistance))
                                                    .setTemporalQueryWindow(
                                                            QueryCondition.TemporalQueryWindow.newBuilder()
                                                                    .setStartMs(temporalQueryWindow.getStartMs())
                                                                    .setEndMs(temporalQueryWindow.getEndMs())
                                                                    .build()
                                                    )
                                                    .build())
                                    .setQueryOperation(QueryCondition.QueryMethod.KNN)
                                    .build();
                } else {
                    knnQueryRequest =
                            QueryCondition.QueryRequest.newBuilder()
                                    .addAllRange(ranges)
                                    .setKnn(
                                            QueryCondition.KNNQueryRequest.newBuilder()
                                                    .setK(knnQueryCondition.getK())
                                                    .setTrajectory(
                                                            ByteString.copyFrom(knnQueryCondition.getTrajectoryBytes()))
                                                    .setDistance(GeoUtils.getDegreeFromKm(maxDistance))
                                                    .build())
                                    .setQueryOperation(QueryCondition.QueryMethod.KNN)
                                    .build();
                }

                return STCoprocessorQuery.executeQuery(targetIndexTable, knnQueryRequest);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    public AbstractQueryCondition generateSTQueryCondition(Geometry geom) throws IOException {
        KNNQueryCondition knnQueryCondition = (KNNQueryCondition) abstractQueryCondition;
        SpatialQueryCondition sqc =
                new SpatialQueryCondition(geom, SpatialQueryCondition.SpatialQueryType.INTERSECT);
        if (hasTimeConstrain()) {
            TemporalQueryCondition tqc = knnQueryCondition.getTemporalQueryCondition();
            return new SpatialTemporalQueryCondition(sqc, tqc);
        } else {
            return sqc;
        }
    }

    public Tuple2<List<RowKeyRange>, HashSet<RowKeyRange>> getSplitRanges(
            AbstractQueryCondition abQc, HashSet<RowKeyRange> set) throws IOException {
        setupTargetIndexTable();
        List<RowKeyRange> partitionScanRanges =
                targetIndexTable.getIndexMeta().getIndexStrategy().getPartitionScanRanges(abQc);
        partitionScanRanges.removeAll(set);
        set.addAll(partitionScanRanges);
        return new Tuple2<>(partitionScanRanges, set);
    }

    @Override
    public JavaRDD<Trajectory> getRDDQuery(SparkSession ss) throws IOException {
        setupTargetIndexTable();
        KNNQueryCondition kqc = (KNNQueryCondition) abstractQueryCondition;
        int k = kqc.getK();
        int resultSearch = 0;
        int searchNothing = 0;
        JavaSparkContext context = SparkUtils.getJavaSparkContext(ss);
        PriorityQueue<TrajectoryWithDistance> pq =
                new PriorityQueue<>(
                        k,
                        (o1, o2) -> {
                            double dist1 = o1.getDistance();
                            double dist2 = o2.getDistance();
                            return Double.compare(dist2, dist1);
                        });
        HashSet<RowKeyRange> set = new HashSet<>();
        switch (kqc.getKnnQueryType()) {
            case Point: {
                BasePoint centralPoint = kqc.getCentralPoint();
                while ((pq.size() < k && searchNothing < MAX_QUERY_NOTHING_TIME) || ( resultSearch != 0)) {
                    Geometry buffer = centralPoint.buffer(GeoUtils.getDegreeFromKm(curSearchDist));

                    AbstractQueryCondition stQc = generateSTQueryCondition(buffer);
                    Tuple2<List<RowKeyRange>, HashSet<RowKeyRange>> ranges = getSplitRanges(stQc, set);
                    List<RowKeyRange> splitRanges = ranges._1;
                    set = ranges._2;
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
                    List<Trajectory> collect = trajRDD.collect();
                    resultSearch = collect.size();
                    if(resultSearch == 0) searchNothing++;
                    addToHeap(collect, pq, kqc);
                    if (!pq.isEmpty()) {
                        maxDistance = pq.peek().getDistance();
                    }
                    getSearchRadiusKM(resultSearch, kqc.getK(), curSearchDist);
                    stage++;
                    logger.info(
                            "Start search radius {} at stage {}, got {} numbers count.",
                            curSearchDist,
                            stage,
                            collect.size());
                }
                return context.parallelize(heapToResultList(pq));
            }
            case Trajectory: {
                Trajectory centralTrajectory = kqc.getCentralTrajectory();
                while ((pq.size() < k && searchNothing < MAX_QUERY_NOTHING_TIME) || (resultSearch != 0)) {
                    Geometry buffer = centralTrajectory.buffer(GeoUtils.getDegreeFromKm(curSearchDist));
                    AbstractQueryCondition stQc = generateSTQueryCondition(buffer);
                    Tuple2<List<RowKeyRange>, HashSet<RowKeyRange>> ranges = getSplitRanges(stQc, set);
                    List<RowKeyRange> splitRanges = ranges._1;
                    set = ranges._2;
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
                    List<Trajectory> collect = trajRDD.collect();
                    resultSearch = collect.size();
                    if(resultSearch == 0) searchNothing++;
                    addToHeap(collect, pq, kqc);
                    if (!pq.isEmpty()) {
                        maxDistance = pq.peek().getDistance();
                    }
                    getSearchRadiusKM(resultSearch, kqc.getK(), curSearchDist);
                    stage++;
                    logger.info(
                            "Start search radius {} at stage {}, got {} numbers count.",
                            curSearchDist,
                            stage,
                            collect.size());
                }
                return context.parallelize(heapToResultList(pq));
            }
            default:
                throw new UnsupportedOperationException();
        }
    }

    private void getSearchRadiusKM(int lastGetRecords, int k, double curSearch) {
        if (lastGetRecords == 0 || k / lastGetRecords <= 10) {
            curSearchDist = curSearch * Math.sqrt(2);
        } else {
            curSearchDist = curSearch * Math.sqrt(k / (double) lastGetRecords);
        }
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
