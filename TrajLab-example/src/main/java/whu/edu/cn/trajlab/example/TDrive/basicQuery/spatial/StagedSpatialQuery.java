package whu.edu.cn.trajlab.example.TDrive.basicQuery.spatial;

import org.apache.hadoop.conf.Configuration;
import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.example.TDrive.basicQuery.CreateQueryWindow;
import whu.edu.cn.trajlab.query.query.StagedQuery;

import java.io.IOException;
import java.util.List;

import static whu.edu.cn.trajlab.query.query.QueryConf.*;

/**
 * @author xuqi
 * @date 2024/05/16
 */
public class StagedSpatialQuery {
    public static void main(String[] args) throws IOException, ParseException {
        String dataSetName = args[0];
        String querySize = args[1];
        String queryWindow = CreateQueryWindow.createSpatialQueryWindow(Double.parseDouble(querySize));
        long start = System.currentTimeMillis();
        Configuration conf = new Configuration();
        conf.set(INDEX_TYPE, "XZ2");
        conf.set(DATASET_NAME, dataSetName);
        conf.set(SPATIAL_WINDOW, queryWindow);
        StagedQuery stagedQuery = new StagedQuery(conf);
        List<Trajectory> scanQuery = stagedQuery.getStagedQuery();
        long end = System.currentTimeMillis();
        long cost = (end - start);
        System.out.println("Data Size : " + scanQuery.size());
        System.out.printf("Query cost %dmin %ds \n", cost / 60000, cost % 60000 / 1000);
    }
}
