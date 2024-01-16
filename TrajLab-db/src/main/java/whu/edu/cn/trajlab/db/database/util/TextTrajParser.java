package whu.edu.cn.trajlab.db.database.util;

import org.locationtech.jts.io.ParseException;
import whu.edu.cn.trajlab.base.trajectory.Trajectory;

/**
 * @author xuqi
 * @date 2023/12/06
 */
public abstract class TextTrajParser {

    public abstract Trajectory parse(String line) throws ParseException;
}
