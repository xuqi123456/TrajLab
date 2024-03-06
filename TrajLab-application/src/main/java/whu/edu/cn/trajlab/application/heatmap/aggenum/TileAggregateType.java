package whu.edu.cn.trajlab.application.heatmap.aggenum;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2024/03/05
 */
public enum TileAggregateType implements Serializable {
    COUNT,
    MAX,
    MIN,
    AVG,
    SUM;
}
