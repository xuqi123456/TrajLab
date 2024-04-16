package whu.edu.cn.trajlab.example.TDrive.basicQuery;

import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import scala.Tuple2;
import whu.edu.cn.trajlab.base.util.GeoUtils;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TimePeriod;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;

/**
 * @author xuqi
 * @date 2024/04/11
 */
public class CreateQueryWindow {
    static Envelope envelope = new Envelope(115.7, 117.4, 39.4,41.6);

    static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
    static ZonedDateTime timeMiddle = ZonedDateTime.parse("2008-02-05 12:00:00", dateTimeFormatter);

    public static String createSpatialQueryWindow(double l) {
        double degree = GeoUtils.getDegreeFromKm(l);

        // 获取envelope的中心点
        Coordinate center = new Coordinate((envelope.getMinX() + envelope.getMaxX()) / 2,
                (envelope.getMinY() + envelope.getMaxY()) / 2);

        // 计算出正方形的左下角点
        Coordinate start = new Coordinate(center.x - degree / 2, center.y - degree / 2);

        // 计算出正方形的右上角点
        Coordinate end = new Coordinate(start.x + degree, start.y + degree);

        // 构建正方形的四个顶点
        Coordinate[] vertices = new Coordinate[] {
                start,
                new Coordinate(start.x, end.y),
                end,
                new Coordinate(end.x, start.y),
                start // 闭合环
        };

        // 使用 JTS GeometryFactory 创建多边形对象
        GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
        Polygon square = geometryFactory.createPolygon(vertices);

        // 将多边形转换为 WKT 格式
        return square.toText();
    }
    public static Tuple2<String, String> createTemporalQueryWindow(double l, TimePeriod timePeriod) {
        TimeLine timeLine = new TimeLine(
                timeMiddle.minus((long) l, timePeriod.getChronoUnit()),
                timeMiddle.plus((long) l, timePeriod.getChronoUnit()));
        // 创建一个 DateTimeFormatter 对象，用于格式化日期时间
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        // 使用 DateTimeFormatter 格式化 ZonedDateTime 对象
        String formatStart = timeLine.getTimeStart().format(formatter);
        String formatEnd = timeLine.getTimeEnd().format(formatter);
        return new Tuple2<>(formatStart, formatEnd);
    }
    public static void main(String[] args){
        Tuple2<String, String> temporalQueryWindow = createTemporalQueryWindow(1, TimePeriod.HOUR);
    System.out.println(temporalQueryWindow);
    }
}
