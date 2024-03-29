package whu.edu.cn.trajlab.core.enums;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * @author xuqi
 * @date 2023/11/20
 */
public enum FileTypeEnum {
    csv("csv"),
    geojson("geojson"),
    wkt("wkt"),
    kml("kml"),
    shp("shp"),
    parquet("parquet");
    private final String fileType;

    FileTypeEnum(String fileType) {
        this.fileType = fileType;
    }

    @JsonValue
    public String getFileTypeEnum() {
        return this.fileType;
    }
}
