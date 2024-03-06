package whu.edu.cn.trajlab.application.heatmap.GridRDD;

import org.apache.spark.api.java.JavaRDD;
import org.locationtech.jts.geom.*;
import scala.Serializable;
import scala.Tuple2;
import whu.edu.cn.trajlab.application.heatmap.aggenum.PyramidAggregateType;
import whu.edu.cn.trajlab.application.heatmap.aggenum.SmoothOperatorType;
import whu.edu.cn.trajlab.application.heatmap.aggenum.TileAggregateType;
import whu.edu.cn.trajlab.application.heatmap.function.AggeFunction;
import whu.edu.cn.trajlab.application.heatmap.tile.Pixel;
import whu.edu.cn.trajlab.application.heatmap.tile.TileGrid;
import whu.edu.cn.trajlab.application.heatmap.tile.TileResult;

/**
 * @author xuqi
 * @date 2024/03/05
 */
public class TileGridDataRDD<T extends Geometry, V> implements Serializable {

    /**
     * Tile data flow and aggregate tile data flow
     */
    private JavaRDD<Tuple2<Pixel, T>> tileDataRDD;

    private JavaRDD<TileResult<V>> tileResultDataRDD;

    private JavaRDD<TileResult<V>> aggregateDataRDD;

    /**
     * Initial level
     */
    public int tileLevel;

    /**
     * Pixel aggregation logic and hierarchical aggregation logic enumeration
     */
    public PyramidAggregateType pyramidAggregateType;

    public TileAggregateType tileAggregateType;

    /**
     * Window smoothing operator
     */
    protected SmoothOperatorType smoothOperator;

    public SmoothOperatorType getSmoothOperator() {
        return smoothOperator;
    }

    public PyramidAggregateType getPyramidAggregateType() {
        return pyramidAggregateType;
    }

    public TileAggregateType getTileAggregateType() {
        return tileAggregateType;
    }

    public JavaRDD<Tuple2<Pixel, T>> getTileDataRDD() {
        return tileDataRDD;
    }

    public JavaRDD<TileResult<V>> getAggregateDataRDD() {
        return aggregateDataRDD;
    }

    public JavaRDD<TileResult<V>> getTileResultDataRDD() {
        return tileResultDataRDD;
    }

    public TileGridDataRDD(
            final JavaRDD<T> spatialDataRDD,
            TileAggregateType tileFlatMapType,
            PyramidAggregateType pyramidAggregateType,
            final int level) {
        this.pyramidAggregateType = pyramidAggregateType;
        this.tileAggregateType = tileFlatMapType;
        this.tileLevel = level;

    tileDataRDD =
        spatialDataRDD
            .map(p -> {
                TileGrid tileGrid = new TileGrid(level);
                if (p instanceof Point) {
                    Point point = (Point) p;
                    return new Tuple2<>(tileGrid.getPixel(point), p);
                } else {
                    throw new IllegalArgumentException("Unsupported geom type");
                }
            });
    }

    public TileGridDataRDD(
            final JavaRDD<T> spatialDataRDD,
            TileAggregateType tileFlatMapType,
            final int level) {
        this.tileAggregateType = tileFlatMapType;
        this.tileLevel = level;

        tileDataRDD = spatialDataRDD
                .map(p -> {
                    TileGrid tileGrid = new TileGrid(level);
                    if (p instanceof Point) {
                        Point point = (Point) p;
                        return new Tuple2<>(tileGrid.getPixel(point), p);
                    } else {
                        throw new IllegalArgumentException("Unsupported geom type");
                    }
                });
    }

    public TileGridDataRDD(
            final JavaRDD<T> spatialDataRDD,
            TileAggregateType tileFlatMapType,
            PyramidAggregateType pyramidAggregateType,
            final int level,
            SmoothOperatorType smoothOperator) {
        this.tileAggregateType = tileFlatMapType;
        this.pyramidAggregateType = pyramidAggregateType;
        this.smoothOperator = smoothOperator;
        this.tileLevel = level;

        tileDataRDD =  spatialDataRDD
                .map(p -> {
                    TileGrid tileGrid = new TileGrid(level);
                    if (p instanceof Point) {
                        Point point = (Point) p;
                        return new Tuple2<>(tileGrid.getPixel(point), p);
                    } else {
                        throw new IllegalArgumentException("Unsupported geom type");
                    }
                });
    }

    public TileGridDataRDD(
            final JavaRDD<T> spatialDataRDD,
            TileAggregateType tileFlatMapType,
            final int level,
            SmoothOperatorType smoothOperator) {
        this.tileAggregateType = tileFlatMapType;
        this.smoothOperator = smoothOperator;
        this.tileLevel = level;

        tileDataRDD =  spatialDataRDD
                .map(p -> {
                    TileGrid tileGrid = new TileGrid(level);
                    if (p instanceof Point) {
                        Point point = (Point) p;
                        return new Tuple2<>(tileGrid.getPixel(point), p);
                    } else {
                        throw new IllegalArgumentException("Unsupported geom type");
                    }
                });
    }

    public TileGridDataRDD(
            int tileLevel,
            TileAggregateType tileFlatMapType,
            PyramidAggregateType pyramidTileAggregateType,
            JavaRDD<TileResult<V>> tileDataRDD,
            Integer hLevel) {
        this.aggregateDataRDD = tileDataRDD;
        for (int fileNumber = tileLevel - 1; fileNumber >= hLevel; fileNumber--) {
            if (fileNumber == tileLevel - 1) {
                tileResultDataRDD = tileDataRDD
                        .mapToPair(t -> new Tuple2<>(t.getTile().getUpperTile(), new Tuple2<>(t.getTile().getUpperTile(), t)))
                        .groupByKey()
                        .mapValues(values -> {
                            AggeFunction.LevelUpAggregate<Geometry, V> upAggregate = new AggeFunction.LevelUpAggregate<>(tileFlatMapType, pyramidTileAggregateType);
                            return upAggregate.aggregate(values);
                        })
                        .map(t -> t._2);
            } else {
                assert tileResultDataRDD != null;
                tileResultDataRDD = tileResultDataRDD
                        .mapToPair(t -> new Tuple2<>(t.getTile().getUpperTile(), new Tuple2<>(t.getTile().getUpperTile(), t)))
                        .groupByKey()
                        .mapValues(values -> {
                            AggeFunction.LevelUpAggregate<Geometry, V> upAggregate = new AggeFunction.LevelUpAggregate<>(tileFlatMapType, pyramidTileAggregateType);
                            return upAggregate.aggregate(values);
                        })
                        .map(t -> t._2);
            }
            aggregateDataRDD = aggregateDataRDD.union(tileResultDataRDD);
        }
    }
}

