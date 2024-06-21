package whu.edu.cn.trajlab.example.TDrive.basicQuery;

import whu.edu.cn.trajlab.db.condition.DataSetQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.Database;
import whu.edu.cn.trajlab.db.database.meta.DataSetMeta;
import whu.edu.cn.trajlab.query.query.basic.DataSetQuery;

import java.io.IOException;


/**
 * @author xuqi
 * @date 2024/04/11
 */
public class TDriveDataSetQuery {
  public static void main(String[] args) throws IOException {
    String dataSetName = args[0];
    Database instance = Database.getInstance();
    DataSet dataSet = instance.getDataSet(dataSetName);
    DataSetQueryCondition dataSetQueryCondition = new DataSetQueryCondition(dataSetName);
    DataSetQuery dataSetQuery = new DataSetQuery(dataSet, dataSetQueryCondition);
    DataSetMeta dataSetMeta = dataSetQuery.getDataSetMeta();
    System.out.println(dataSetMeta.toString());
  }
}
