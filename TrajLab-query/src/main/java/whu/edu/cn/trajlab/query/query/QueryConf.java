package whu.edu.cn.trajlab.query.query;

import org.apache.hadoop.conf.Configured;

public class QueryConf extends Configured {
    //Common Query
    public static String INDEX_TYPE = "INDEX_TYPE";
    public static String DATASET_NAME = "DATASET_NAME";

    //Basic Query
    public static String SPATIAL_WINDOW = "SPATIAL_WINDOW";
    public static String START_TIME = "START_TIME";
    public static String END_TIME = "END_TIME";
    public static String OID = "OID";

    //Advance query
    public static String CENTER_POINT = "CENTER_POINT";
    public static String CENTER_TRAJECTORY = "CENTER_TRAJECTORY";
    public static String K = "K";
    public static String DIS_THRESHOLD = "DIS_THRESHOLD";
    public static String TIME_THRESHOLD = "TIME_THRESHOLD";
    public static String TIME_PERIOD = "TIME_PERIOD";

}
