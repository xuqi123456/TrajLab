package whu.edu.cn.trajlab.example.TDrive.basicQuery;

import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.IDQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.query.query.basic.IDQuery;

import java.io.IOException;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/04/13
 */
public class TDriveIDQuery {
    public static Trajectory getCoreTrajectory(String dataSetName, String moid) throws IOException {
        Database instance = Database.getInstance();
        DataSet dataSet = instance.getDataSet(dataSetName);
        IDQueryCondition idQueryCondition = new IDQueryCondition(moid);
        IDQuery idQuery = new IDQuery(dataSet, idQueryCondition);
        List<Trajectory> trajectories = idQuery.executeQuery();
        return trajectories.get(0);
    }
}
