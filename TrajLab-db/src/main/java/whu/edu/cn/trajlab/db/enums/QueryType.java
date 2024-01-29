package whu.edu.cn.trajlab.db.enums;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/12/10
 */
public enum QueryType implements Serializable {
    SPATIAL("spatial"),
    TEMPORAL("temporal"),
    ID("id"),
    ST("st"),
    ID_T("id-temporal"),
    DATASET("dataset"),
    KNN("knn"),
    SIMILAR("similar"),
    BUFFER("buffer"),
    ACCOMPANY("Accompany");
    private String queryType;

    QueryType(String queryType) {
        this.queryType = queryType;
    }

    @Override
    public String toString() {
        return "QueryType{" +
                "queryType='" + queryType + '\'' +
                '}';
    }
}
