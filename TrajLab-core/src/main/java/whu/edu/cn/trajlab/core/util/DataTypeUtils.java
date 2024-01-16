package whu.edu.cn.trajlab.core.util;

import whu.edu.cn.trajlab.core.common.field.Field;
import whu.edu.cn.trajlab.core.enums.BasicDataTypeEnum;

/**
 * @author xuqi
 * @date 2023/11/15
 */
public class DataTypeUtils {
    public static Object parse(String tarStr, BasicDataTypeEnum dataType) {
        return parse(tarStr, dataType, null);
    }

    public static Object parse(String rawValue, BasicDataTypeEnum dataType, Field field) {
        switch (dataType) {
            case STRING:
                return rawValue;
            case INT:
                return Integer.parseInt(rawValue);
            case LONG:
                return Long.parseLong(rawValue);
            case DATE:
            case TIMESTAMP:
                return BasicDateUtils.parse(dataType, rawValue, field);
            case DOUBLE:
                return Double.parseDouble(rawValue);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
