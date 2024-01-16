package whu.edu.cn.trajlab.db.condition;


import whu.edu.cn.trajlab.db.enums.QueryType;

/**
 * @author xuqi
 * @date 2023/12/11
 */
public class DataSetQueryCondition extends AbstractQueryCondition{
    String dataSetName;

    public DataSetQueryCondition(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    public String getDataSetName() {
        return dataSetName;
    }

    @Override
    public String getConditionInfo() {
        return "DataSetQueryCondition{" +
                "dataSetName='" + dataSetName + '\'' +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.DATASET;
    }
}
