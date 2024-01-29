package whu.edu.cn.trajlab.db.enums;

import java.io.Serializable;

public enum KNNQueryType implements Serializable {
    Point("point"),
    Trajectory("trajectory");
    private String queryType;

    KNNQueryType(String queryType) {
        this.queryType = queryType;
    }

    @Override
    public String toString() {
        return "QueryType{" +
                "queryType='" + queryType + '\'' +
                '}';
    }

}
