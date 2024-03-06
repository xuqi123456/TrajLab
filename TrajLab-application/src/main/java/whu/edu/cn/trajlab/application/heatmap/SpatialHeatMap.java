package whu.edu.cn.trajlab.application.heatmap;

import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.Geometry;
import whu.edu.cn.trajlab.application.heatmap.GridRDD.TileGridDataRDD;
import whu.edu.cn.trajlab.application.heatmap.function.AggeFunction;
import whu.edu.cn.trajlab.application.heatmap.tile.TileResult;

/**
 * @author xuqi
 * @date 2024/03/05
 */
public class SpatialHeatMap{
    public static <T extends Geometry, V> JavaRDD<TileResult<V>> heatmap(
            TileGridDataRDD<T, V> tileGridDataRDD,
            int hLevel) {
        JavaRDD<TileResult<V>> tileResultDataRDD = tileGridDataRDD
                .getTileDataRDD()
                .groupBy(t -> t._1.getTile())
                .mapValues(values -> {
                    AggeFunction.TileAggregate<T, V> aggregate = new AggeFunction.TileAggregate<>(tileGridDataRDD.getTileAggregateType(),
                            tileGridDataRDD.getSmoothOperator());
                    return aggregate.aggregate(values);
                })
                .map(t -> t._2);


        TileGridDataRDD<T, V> tileGridDataStream1 = new TileGridDataRDD<T, V>(
                tileGridDataRDD.tileLevel,
                tileGridDataRDD.getTileAggregateType(),
                tileGridDataRDD.getPyramidAggregateType(),
                tileResultDataRDD,
                hLevel);
        return tileGridDataStream1.getAggregateDataRDD();
    }
}
