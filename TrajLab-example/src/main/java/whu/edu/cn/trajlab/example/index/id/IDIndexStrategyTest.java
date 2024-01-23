package whu.edu.cn.trajlab.example.index.id;

import junit.framework.TestCase;
import whu.edu.cn.trajlab.db.coding.coding.XZTCoding;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.condition.IDTemporalQueryCondition;
import whu.edu.cn.trajlab.db.condition.TemporalQueryCondition;
import whu.edu.cn.trajlab.db.enums.TemporalQueryType;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.db.index.id.IDIndexStrategy;
import whu.edu.cn.trajlab.db.index.time.IDTIndexStrategy;

import java.util.List;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class IDIndexStrategyTest extends TestCase {
  public void testGetSingleID() {
    String Oid = "001";
    IDQueryCondition idQueryCondition = new IDQueryCondition(Oid);
    IDIndexStrategy idIndexStrategy = new IDIndexStrategy();
    List<RowKeyRange> scanRanges = idIndexStrategy.getScanRanges(idQueryCondition);
    System.out.println("Single ID-Time Range:");
    for (RowKeyRange scanRange : scanRanges) {
      System.out.println(
          "start : "
              + idIndexStrategy.parseScanIndex2String(scanRange.getStartKey())
              + " end : "
              + idIndexStrategy.parseScanIndex2String(scanRange.getEndKey())
              + " isContained "
              + scanRange.isValidate());
    }
  }
}
