package whu.edu.cn.trajlab.example.index.time;

import junit.framework.TestCase;
import whu.edu.cn.trajlab.db.coding.coding.XZTCoding;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.datatypes.TimeLine;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.db.index.time.TemporalIndexStrategy;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static whu.edu.cn.trajlab.db.constant.CodingConstants.TIME_ZONE;

/**
 * @author xuqi
 * @date 2024/01/22
 */
public class TIndexStrategyTest extends TestCase {
    public void testGetSingleScanRanges() {
        DateTimeFormatter dateTimeFormatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(TIME_ZONE);
        ZonedDateTime start = ZonedDateTime.parse("2015-12-26 12:00:00", dateTimeFormatter);
        ZonedDateTime end = ZonedDateTime.parse("2015-12-26 13:00:00", dateTimeFormatter);
        TimeLine timeLine = new TimeLine(start, end);
        TemporalQueryCondition temporalQueryCondition = new TemporalQueryCondition(timeLine, TemporalQueryType.INTERSECT);
        TemporalIndexStrategy temporalIndexStrategy = new TemporalIndexStrategy(new XZTCoding());
        List<RowKeyRange> scanRanges = temporalIndexStrategy.getPartitionScanRanges(temporalQueryCondition);
        System.out.println("Single ID-Time Range:");
        for (RowKeyRange scanRange : scanRanges) {
            System.out.println(
                    "start : "
                            + temporalIndexStrategy.parsePhysicalIndex2String(scanRange.getStartKey())
                            + " end : "
                            + temporalIndexStrategy.parsePhysicalIndex2String(scanRange.getEndKey())
                            + " isContained "
                            + scanRange.isValidate());
        }
    }
}
