package whu.edu.cn.trajlab.query.query.advanced;

import whu.edu.cn.trajlab.base.trajectory.Trajectory;
import whu.edu.cn.trajlab.db.condition.AbstractQueryCondition;
import whu.edu.cn.trajlab.db.database.DataSet;
import whu.edu.cn.trajlab.db.database.meta.IndexMeta;
import whu.edu.cn.trajlab.db.database.table.IndexTable;
import whu.edu.cn.trajlab.db.index.RowKeyRange;
import whu.edu.cn.trajlab.query.query.basic.AbstractQuery;

import java.io.IOException;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/01/29
 */
public class BufferQuery extends AbstractQuery {
    public BufferQuery(DataSet dataSet, AbstractQueryCondition abstractQueryCondition) {
        super(dataSet, abstractQueryCondition);
    }

    public BufferQuery(IndexTable targetIndexTable, AbstractQueryCondition abstractQueryCondition) throws IOException {
        super(targetIndexTable, abstractQueryCondition);
    }

    @Override
    public List<Trajectory> executeQuery(List<RowKeyRange> rowKeyRanges) throws IOException {
        return null;
    }

    @Override
    public IndexMeta findBestIndex() {
        return null;
    }

    @Override
    public String getQueryInfo() {
        return null;
    }
}
