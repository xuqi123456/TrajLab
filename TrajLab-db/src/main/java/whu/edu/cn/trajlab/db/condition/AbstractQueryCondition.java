package whu.edu.cn.trajlab.db.condition;


import whu.edu.cn.trajlab.db.enums.QueryType;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public abstract class AbstractQueryCondition implements Serializable {
    public abstract String getConditionInfo();
    public abstract QueryType getInputType();
}
