package whu.edu.cn.trajlab.example.query.basic;

import junit.framework.TestCase;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.query.query.basic.IDQuery;

import java.io.IOException;
import java.util.List;

import static whu.edu.cn.trajlab.example.query.basic.SpatialQueryTest.DATASET_NAME;

/**
 * @author xuqi
 * @date 2024/01/23
 */
public class IDQueryTest extends TestCase{

    public void testIDQuery() throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(DATASET_NAME);
        IDQueryCondition idQueryCondition = new IDQueryCondition("001");
        IDQuery idQuery = new IDQuery(dataSet, idQueryCondition);
        List<Trajectory> trajectories = idQuery.executeQuery();
        System.out.println(trajectories.size());
        for (Trajectory trajectory : trajectories) {
            System.out.println(trajectory);
        }
        assertEquals(23, trajectories.size());
    }
}
