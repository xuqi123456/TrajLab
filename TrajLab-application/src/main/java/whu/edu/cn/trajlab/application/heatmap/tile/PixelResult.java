package whu.edu.cn.trajlab.application.heatmap.tile;

/**
 * @author xuqi
 * @date 2024/03/05
 */
public class PixelResult<V> {

    private Pixel pixel;
    private V result;

    public PixelResult(Pixel pixel, V result) {
        this.pixel = pixel;
        this.result = result;
    }

    public Pixel getPixel() {
        return pixel;
    }

    public void setPixel(Pixel pixel) {
        this.pixel = pixel;
    }

    public V getResult() {
        return result;
    }

    public void setResult(V result) {
        this.result = result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PixelResult pixelResult = (PixelResult) o;
        return pixel.equals(pixelResult.pixel) && result.equals(pixelResult.result);
    }
    @Override
    public int hashCode() {
        return 0;
    }

}
