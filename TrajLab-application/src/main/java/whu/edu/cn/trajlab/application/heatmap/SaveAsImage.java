package whu.edu.cn.trajlab.application.heatmap;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;
import whu.edu.cn.trajlab.application.heatmap.tile.Pixel;
import whu.edu.cn.trajlab.application.heatmap.tile.PixelResult;
import whu.edu.cn.trajlab.application.heatmap.tile.Tile;
import whu.edu.cn.trajlab.application.heatmap.tile.TileResult;

import javax.imageio.ImageIO;
import java.awt.*;
import java.awt.image.BufferedImage;
import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * @author xuqi
 * @date 2024/03/06
 */
public class SaveAsImage<V extends Number> implements Serializable {
    private int width = 256;
    private int height = 256;
    private int v0 = 10;
    private String outputPath;

    public SaveAsImage(int width, int height) {
        this.width = width;
        this.height = height;
    }

    public SaveAsImage(int width, int height, int v0) {
        this.width = width;
        this.height = height;
        this.v0 = v0;
    }

    public SaveAsImage(int width, int height, int v0, String outputPath) {
        this.width = width;
        this.height = height;
        this.v0 = v0;
        this.outputPath = outputPath;
    }

    public SaveAsImage(String outputPath) {
        this.outputPath = outputPath;
    }

    public void saveHeatMapAsImage(JavaRDD<TileResult<V>> heatmapRDD){
        // 将每个像素矩阵转换为 BufferedImage 对象
        JavaPairRDD<BufferedImage, Tile> images = heatmapRDD.mapToPair(matrix -> {
            List<PixelResult<V>> gridResultList = matrix.getGridResultList();
            BufferedImage image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB);
            for (PixelResult<V> vPixelResult : gridResultList) {
                Pixel pixel = vPixelResult.getPixel();
                V result = vPixelResult.getResult();
                int rgba = getRGBA(result);
                image.setRGB(pixel.getPixelX(), pixel.getPixelY(), rgba);
            }
            Tile tile = matrix.getTile();

            return new Tuple2<>(image, tile);
        });

        // 保存图片文件
        saveImagesToFiles(images);
    }
    public int getRGBA(V result){
        double limit = Math.log((Double) result * 10 + 1) / Math.log(v0);
        int red = (int) Math.floor(255 * Math.min(1, limit));
        int green = (int) Math.floor(255 * Math.min(1, Math.max(0, limit - 1)));
        int blue = (int) Math.floor(255 * Math.min(1, Math.max(0, limit - 2)));
        int alpha = (int) Math.min(1, limit);
        Color color = new Color(red, green, blue);
//        png图片生成方法
//        int argb = (alpha << 24) | (red << 16) | (green << 8) | blue;
//        return argb & 0x00FFFFFF;
        return color.getRGB();
    }
    public void saveImagesToFiles(JavaPairRDD<BufferedImage, Tile> imageRDD) {
        // 判断目录是否存在，如果不存在则创建目录
        File directory = new File(outputPath);
        if (!directory.exists()) directory.mkdirs();
        imageRDD.foreach(image -> {
            try {
                Tile tile = image._2;
                File output = new File( outputPath + "/" + tile.getLevel() + "_" + tile.getX() + "_" + tile.getY() + ".jpg");
                ImageIO.write(image._1, "jpg", output);
                System.out.println("Image saved to: " + output.getAbsolutePath());
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}
