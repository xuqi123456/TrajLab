package whu.edu.cn.trajlab.query.util;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * @author xuqi
 * @date 2024/01/16
 */
public class BasicDateUtils {
    public static final ZoneId DEFAULT_ZONE_ID = ZoneId.of("UTC+8");
    public static ZonedDateTime timeToZonedTime(long time) {
        return ZonedDateTime.ofInstant(Instant.ofEpochSecond(time),
                DEFAULT_ZONE_ID);
    }
}
