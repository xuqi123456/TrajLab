package whu.edu.cn.trajlab.base.util;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author xuqi
 * @date 2024/01/15
 */
public class DateUtils {
    static String deaultFormat = "yyyy-MM-dd HH:mm:ss";
    static String defaultZoneId = "UTC+8";

    static DateTimeFormatter defaultFormatter =
            DateTimeFormatter.ofPattern(deaultFormat).withZone(ZoneId.of(defaultZoneId));
    public static String getDeaultFormat() {
        return deaultFormat;
    }

    public static String getDefaultZoneId() {
        return defaultZoneId;
    }

    public static DateTimeFormatter getDefaultFormatter() {
        return defaultFormatter;
    }
    public static ZonedDateTime parseDate(String timeFormat) {
        return ZonedDateTime.parse(timeFormat, defaultFormatter);
    }
    public static String format(ZonedDateTime time, String pattern) {
        return time == null ? "" : DateTimeFormatter.ofPattern(pattern).format(time);
    }
    public static long parseDateToTimeStamp(ZonedDateTime dateTime) {
        return dateTime.toEpochSecond();
    }
}
