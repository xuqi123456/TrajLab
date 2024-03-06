package whu.edu.cn.trajlab.application.heatmap.function;

import org.locationtech.jts.geom.Geometry;
import scala.Tuple2;
import scala.Tuple3;
import whu.edu.cn.trajlab.application.heatmap.aggenum.PyramidAggregateType;
import whu.edu.cn.trajlab.application.heatmap.aggenum.SmoothOperatorType;
import whu.edu.cn.trajlab.application.heatmap.aggenum.TileAggregateType;
import whu.edu.cn.trajlab.application.heatmap.tile.Pixel;
import whu.edu.cn.trajlab.application.heatmap.tile.PixelResult;
import whu.edu.cn.trajlab.application.heatmap.tile.Tile;
import whu.edu.cn.trajlab.application.heatmap.tile.TileResult;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * @author xuqi
 * @date 2024/03/05
 */
public class AggeFunction {

    public static class TileAggregate<T extends Geometry, V> {

        private final TileAggregateType tileFlatMapType;
        private final SmoothOperatorType smoothOperator;
        Double weight = 1.0;

        private final Map<Pixel, Tuple3<Double, HashSet<String>, Integer>> pixelIntegerMap = new HashMap<>();
        public TileAggregate(TileAggregateType tileFlatMapType, SmoothOperatorType smoothOperator) {
            this.tileFlatMapType = tileFlatMapType;
            this.smoothOperator = smoothOperator;
        }

        public TileResult<V> aggregate(
                Iterable<Tuple2<Pixel, T>> values) {
            for (Tuple2<Pixel,T> inPixel : values) {
                Pixel pixel = inPixel._1;
                String carNo = (String) inPixel._2.getUserData();
                try {
                    if (!pixelIntegerMap.containsKey(pixel)) {
                        HashSet<String> carNos = new HashSet<>();
                        carNos.add(carNo);
                        pixelIntegerMap.put(pixel, new Tuple3<>(weight, carNos, 1));
                    } else if (!pixelIntegerMap.get(pixel)._2().contains(carNo)) {
                        pixelIntegerMap.get(pixel)._2().add(carNo);
                        Tuple3<Double, HashSet<String>, Integer> tuple3 = pixelIntegerMap.get(pixel);
                        switch (tileFlatMapType) {
                            case MAX:
                                pixelIntegerMap.put(pixel, new Tuple3<>(Math.max(tuple3._1(), weight), tuple3._2(), tuple3._3()));
                                break;
                            case MIN:
                                pixelIntegerMap.put(pixel, new Tuple3<>(Math.min(tuple3._1(), weight), tuple3._2(), tuple3._3()));
                                break;
                            case COUNT:
                                pixelIntegerMap.put(pixel,new Tuple3<>(1.0, tuple3._2(), tuple3._3()));
                                break;
                            case AVG:
                            case SUM:
                                pixelIntegerMap.put(pixel, new Tuple3<>(tuple3._1() + weight, tuple3._2(), tuple3._3()+1));
                                break;
                            default:
                                throw new IllegalArgumentException("Illegal tileFlatMap type");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return getResult();
        }

        public TileResult<V> getResult() {
            TileResult<V> ret = new TileResult<>();
            Map<Pixel, Double> temple = new HashMap<>();
            ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
            for (Map.Entry<Pixel, Tuple3<Double, HashSet<String>, Integer>> entry : pixelIntegerMap.entrySet()) {
                double finalValue;
                if (tileFlatMapType == TileAggregateType.AVG) {
                    finalValue = entry.getValue()._1() / entry.getValue()._3();
                } else {
                    finalValue = entry.getValue()._1();
                }
                temple.put(entry.getKey(), finalValue);
            }
            if (smoothOperator != null) {
                Map<Tuple2<Integer, Integer>, Double> mapOperator = SmoothOperatorType.
                        getMapOperator(smoothOperator);
                Double defineValue = 0.0;
                for (Map.Entry<Pixel, Double> pixelDoubleEntry : temple.entrySet()) {
                    if (pixelDoubleEntry.getKey().getPixelX() >= (SmoothOperatorType.getLength() / 2)
                            && (pixelDoubleEntry.getKey().getPixelX() <= 255 - (SmoothOperatorType.getLength() / 2))
                            && (pixelDoubleEntry.getKey().getPixelY() >= (SmoothOperatorType.getLength() / 2))
                            && (pixelDoubleEntry.getKey().getPixelY() <= 255 - (SmoothOperatorType.getLength() / 2))) {
                        for (Map.Entry<Tuple2<Integer, Integer>, Double> doubleEntry : mapOperator.entrySet()) {
                            int modelX = pixelDoubleEntry.getKey().getPixelX() + doubleEntry.getKey()._1;
                            int modelY = pixelDoubleEntry.getKey().getPixelY() + doubleEntry.getKey()._2;
                            double modelValue = 0.0;
                            for (Map.Entry<Pixel, Double> smooth : temple.entrySet()) {
                                if (smooth.getKey().getPixelNo() == modelX + modelY * 256) {
                                    modelValue = smooth.getValue();
                                }
                            }
                            defineValue += modelValue * doubleEntry.getValue();
                        }
                        ret.addPixelResult(new PixelResult<>(pixelDoubleEntry.getKey(), (V) defineValue));
                        defineValue = 0.0;
                    } else {
                        ret.addPixelResult(new PixelResult<>(pixelDoubleEntry.getKey(), (V) pixelDoubleEntry.getValue()));
                    }
                }
            } else {
                for (Map.Entry<Pixel, Double> entry : temple.entrySet()) {
                    ret.addPixelResult(new PixelResult<>(entry.getKey(), (V) entry.getValue()));
                }
            }
            return ret;
        }
    }

    public static class LevelUpAggregate<T extends Geometry, V> {

        private final TileAggregateType tileFlatMapType;
        private final PyramidAggregateType pyramidTileAggregateType;
        private final Map<Pixel, Double> pixelIntegerMap = new HashMap<>();

        public LevelUpAggregate(TileAggregateType tileFlatMapType,
                                PyramidAggregateType pyramidTileAggregateType) {
            this.tileFlatMapType = tileFlatMapType;
            this.pyramidTileAggregateType = pyramidTileAggregateType;
        }

        public TileResult<V> aggregate(Iterable<Tuple2<Tile, TileResult<V>>> values) {
            for (Tuple2<Tile, TileResult<V>> tileResultTileTuple2 : values) {
                Tile lowTile = tileResultTileTuple2._2.getTile();
                Tile upperTile = tileResultTileTuple2._1;
                int length = tileResultTileTuple2._2.getGridResultList().size();
                int newPixelNo = 0, newPixelX = 0, newPixelY = 0;
                for (int i = 0; i < length; i++) {
                    PixelResult<V> pixelValue = tileResultTileTuple2._2.getGridResultList().get(i);
                    int pixelY = pixelValue.getPixel().getPixelY();
                    int pixelX = pixelValue.getPixel().getPixelX();
                    if (((double) lowTile.getY() / 2) - upperTile.getY() == 0
                            & ((double) lowTile.getX() / 2 - upperTile.getX() == 0)) {
                        newPixelX = pixelX / 2;
                        newPixelY = pixelY / 2;
                    } else if (((double) lowTile.getY() / 2) - upperTile.getY() > 0
                            & ((double) lowTile.getX() / 2 - upperTile.getX() == 0)) {
                        newPixelX = pixelX / 2;
                        newPixelY = pixelY / 2 + 128;
                    } else if (((double) lowTile.getY() / 2) - upperTile.getY() == 0
                            & ((double) lowTile.getX() / 2 - upperTile.getX() > 0)) {
                        newPixelX = pixelX / 2 + 128;
                        newPixelY = pixelY / 2;
                    } else if (((double) lowTile.getY() / 2) - upperTile.getY() > 0
                            & ((double) lowTile.getX() / 2 - upperTile.getX() > 0)) {
                        newPixelX = pixelX / 2 + 128;
                        newPixelY = pixelY / 2 + 128;
                    }
                    newPixelNo = newPixelY * 256 + newPixelX;
                    Double value = (Double) pixelValue.getResult();
                    Pixel newPixel = new Pixel(upperTile, newPixelX, newPixelY, newPixelNo);
                    if (!pixelIntegerMap.containsKey(newPixel)) {
                        pixelIntegerMap.put(newPixel, value);
                    } else {
                        if (pyramidTileAggregateType != null) {
                            switch (pyramidTileAggregateType) {
                                case MIN:
                                    pixelIntegerMap.put(newPixel, Math.min(pixelIntegerMap.get(newPixel), value));
                                    break;
                                case MAX:
                                    pixelIntegerMap.put(newPixel, Math.max(pixelIntegerMap.get(newPixel), value));
                                    break;
                                case AVG:
                                case SUM:
                                    pixelIntegerMap.put(newPixel, pixelIntegerMap.get(newPixel) + value);
                                    break;
                                case COUNT:
                                    pixelIntegerMap.put(newPixel, 1.0);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Illegal PyramidTileAggre Type");
                            }
                        } else {
                            switch (tileFlatMapType) {
                                case MIN:
                                    pixelIntegerMap.put(newPixel, Math.min(pixelIntegerMap.get(newPixel), value));
                                    break;
                                case MAX:
                                    pixelIntegerMap.put(newPixel, Math.max(pixelIntegerMap.get(newPixel), value));
                                    break;
                                case AVG:
                                case SUM:
                                    pixelIntegerMap.put(newPixel, pixelIntegerMap.get(newPixel) + value);
                                    break;
                                case COUNT:
                                    pixelIntegerMap.put(newPixel, 1.0);
                                    break;
                                default:
                                    throw new IllegalArgumentException("Illegal PyramidTileAggre Type");
                            }
                        }
                    }
                }
            }

            return getResult();
        }

        public TileResult<V> getResult() {
            TileResult<V> ret = new TileResult<>();
            ret.setTile(pixelIntegerMap.keySet().iterator().next().getTile());
            Double finalvalue;
            for (Map.Entry<Pixel, Double> entry : pixelIntegerMap.entrySet()) {
                if (pyramidTileAggregateType == PyramidAggregateType.AVG) {
                    finalvalue = entry.getValue() / 4;
                } else {
                    finalvalue = entry.getValue();
                }
                ret.addPixelResult(new PixelResult<>(entry.getKey(), (V) finalvalue));
            }
            return ret;
        }
    }
}
