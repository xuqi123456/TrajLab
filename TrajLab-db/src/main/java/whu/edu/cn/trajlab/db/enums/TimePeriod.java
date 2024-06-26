package whu.edu.cn.trajlab.db.enums;

import java.time.temporal.ChronoUnit;

/**
 * @author xuqi
 * @date 2023/11/29
 */
public enum TimePeriod {
    MINUTE(ChronoUnit.MINUTES),
    HOUR(ChronoUnit.HOURS),
    DAY(ChronoUnit.DAYS),
    WEEK(ChronoUnit.WEEKS),
    MONTH(ChronoUnit.MONTHS),
    YEAR(ChronoUnit.YEARS);

    ChronoUnit chronoUnit;

    TimePeriod(ChronoUnit chronoUnit) {
        this.chronoUnit = chronoUnit;
    }

    public ChronoUnit getChronoUnit() {
        return chronoUnit;
    }

    @Override
    public String toString() {
        return "TimePeriod{" +
                "chronoUnit=" + chronoUnit +
                '}';
    }
}
