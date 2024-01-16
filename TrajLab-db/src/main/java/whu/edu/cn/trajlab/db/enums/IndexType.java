package whu.edu.cn.trajlab.db.enums;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/12/01
 */
public enum IndexType implements Serializable {
    // spatial only
    XZ2(0),
    Temporal(1),
    ID(2),
    // Concatenate temporal index before spatial index
    TXZ2(3),
    // Concatenate spatial index before temporal index
    XZ2T(4),
    // Index value will be car ids
    OBJECT_ID_T(5);

    int id;

    public static List<IndexType> spatialIndexTypes() {
        return Arrays.asList(XZ2, XZ2T, TXZ2);
    }

    IndexType(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }
}