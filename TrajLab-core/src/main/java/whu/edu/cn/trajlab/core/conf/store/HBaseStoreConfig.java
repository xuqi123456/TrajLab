package whu.edu.cn.trajlab.core.conf.store;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import whu.edu.cn.trajlab.db.database.meta.DataSetMeta;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.core.enums.FileSplitterEnum;
import whu.edu.cn.trajlab.db.enums.IndexType;
import whu.edu.cn.trajlab.core.enums.StoreSchemaEnum;
import whu.edu.cn.trajlab.db.index.id.IDIndexStrategy;
import whu.edu.cn.trajlab.db.index.spatial.XZ2IndexStrategy;
import whu.edu.cn.trajlab.db.index.spatialtemporal.TXZ2IndexStrategy;
import whu.edu.cn.trajlab.db.index.spatialtemporal.XZ2TIndexStrategy;
import whu.edu.cn.trajlab.db.index.time.IDTIndexStrategy;
import scala.NotImplementedError;
import whu.edu.cn.trajlab.db.index.time.TemporalIndexStrategy;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @author xuqi
 * @date 2023/11/14
 */
public class HBaseStoreConfig implements IStoreConfig {

    private final String location;
    private final String dataSetName;
    private final IndexType mainIndex;
    private String otherIndex;
    private final StoreSchemaEnum schema;
    private final List<IndexMeta> indexList;
    private final DataSetMeta dataSetMeta;

    @JsonCreator
    public HBaseStoreConfig(
            @JsonProperty("location") String location,
            @JsonProperty("dataSetName") String dataSetName,
            @JsonProperty("schema") StoreSchemaEnum schema,
            @JsonProperty("mainIndex") IndexType mainIndex,
            @JsonProperty("otherIndex") @JsonInclude(JsonInclude.Include.NON_NULL) String otherIndex) {
        this.location = location;
        this.dataSetName = dataSetName;
        this.schema = schema;
        this.mainIndex = mainIndex;
        this.otherIndex = otherIndex;
        this.indexList = createIndexList();
        this.dataSetMeta = new DataSetMeta(this.dataSetName, this.indexList);
    }

    @Override
    public StoreTypeEnum getStoreType() {
        return StoreTypeEnum.HBASE;
    }

    public String getLocation() {
        return location;
    }

    public String getDataSetName() {
        return dataSetName;
    }

    public IndexType getMainIndex() {
        return mainIndex;
    }

    public String getOtherIndex() {
        return otherIndex;
    }

    public StoreSchemaEnum getSchema() {
        return schema;
    }

    public List<IndexMeta> getIndexList() {
        return indexList;
    }

    public DataSetMeta getDataSetMeta() {
        return dataSetMeta;
    }

    public void setOtherIndex(String otherIndex) {
        this.otherIndex = otherIndex;
    }

    private List<IndexMeta> createIndexList() {
        List<IndexMeta> indexMetaList = new LinkedList<>();
        IndexMeta mainIndexMeta = createIndexMeta(mainIndex, true);
        indexMetaList.add(mainIndexMeta);
        if (otherIndex != null) {
            List<IndexMeta> otherIndexMeta = createOtherIndex(otherIndex, FileSplitterEnum.CSV);
            indexMetaList.addAll(otherIndexMeta);
        }
        checkIndexMeta(indexMetaList, mainIndexMeta);
        return indexMetaList;
    }
    private void checkIndexMeta(List<IndexMeta> indexMetaList, IndexMeta mainIndexMeta){
        // 检查重复
        HashSet<IndexMeta> hashSet = new HashSet<>(indexMetaList);
        if (hashSet.size() != indexMetaList.size()) {
            throw new IllegalArgumentException("found duplicate index meta in the list.");
        }
    }

    private IndexMeta createIndexMeta(IndexType indexType, Boolean isMainIndex) {
        switch (indexType) {
            case XZ2:
                return new IndexMeta(isMainIndex, new XZ2IndexStrategy(), dataSetName, "default");
            case TXZ2:
                return new IndexMeta(isMainIndex, new TXZ2IndexStrategy(), dataSetName, "default");
            case XZ2T:
                return new IndexMeta(isMainIndex, new XZ2TIndexStrategy(), dataSetName, "default");
            case OBJECT_ID_T:
                return new IndexMeta(isMainIndex, new IDTIndexStrategy(), dataSetName, "default");
            case Temporal:
                return new IndexMeta(isMainIndex, new TemporalIndexStrategy(), dataSetName, "default");
            case ID:
                return new IndexMeta(isMainIndex, new IDIndexStrategy(), dataSetName, "default");
            default:
                throw new NotImplementedError();
        }
    }

    private List<IndexMeta> createOtherIndex(String otherIndex, FileSplitterEnum splitType) {
        String[] indexValue = otherIndex.split(splitType.getDelimiter());
        ArrayList<IndexMeta> indexMetaList = new ArrayList<>();
        for (String index : indexValue) {
            IndexType indexType = IndexType.valueOf(index);
            IndexMeta indexMeta = createIndexMeta(indexType, false);
            indexMetaList.add(indexMeta);
        }
        return indexMetaList;
    }

    @Override
    public String toString() {
        return "HBaseStoreConfig{" +
                "mainIndex=" + mainIndex +
                ", otherIndex='" + otherIndex + '\'' +
                ", dataSetMeta=" + dataSetMeta +
                '}';
    }
}
