package whu.edu.cn.trajlab.application.Tracluster.dbscan.partition;

import java.util.Objects;

/**
 * @author xuqi
 * @date 2024/03/02
 */
public class Grid {
    private long x;
    private long y;

    public Grid(long x, long y) {
        this.x = x;
        this.y = y;
    }

    public long getX() {
        return x;
    }

    public long getY() {
        return y;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Grid grid = (Grid) o;
        return x == grid.x && y == grid.y;
    }

    @Override
    public int hashCode() {
        return Objects.hash(x, y);
    }
}
