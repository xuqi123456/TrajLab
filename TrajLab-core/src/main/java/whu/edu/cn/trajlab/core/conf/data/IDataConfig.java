package whu.edu.cn.trajlab.core.conf.data;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import whu.edu.cn.trajlab.core.enums.DataTypeEnum;

import java.io.Serializable;

/**
 * @author xuqi
 * @date 2023/11/14
 */
@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME
)
@JsonSubTypes({@JsonSubTypes.Type(
        value = TrajectoryConfig.class,
        name = "whu/edu/cn/trajlab/base/trajectory"
), @JsonSubTypes.Type(
        value = TrajPointConfig.class,
        name = "traj_point"
)})
public interface IDataConfig extends Serializable {
    DataTypeEnum getDataType();
}
