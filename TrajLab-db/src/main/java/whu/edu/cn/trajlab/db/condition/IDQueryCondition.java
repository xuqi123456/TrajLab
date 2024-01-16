package whu.edu.cn.trajlab.db.condition;


import whu.edu.cn.trajlab.db.enums.QueryType;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public class IDQueryCondition extends AbstractQueryCondition{

    String moid;

    public IDQueryCondition(String moid) {
        this.moid = moid;
    }

    public String getMoid() {
        return moid;
    }

    @Override
    public String getConditionInfo() {
        return "IDQueryCondition{" +
                "moid='" + moid + '\'' +
                '}';
    }

    @Override
    public QueryType getInputType() {
        return QueryType.ID;
    }
}
