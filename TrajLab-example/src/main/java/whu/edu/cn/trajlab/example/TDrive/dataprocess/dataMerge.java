package whu.edu.cn.trajlab.example.TDrive.dataprocess;

import java.io.*;


/**
 * @author xuqi
 * @date 2024/04/09
 */
public class dataMerge {
  public static void main(String[] args) throws IOException {
    String path = "D:\\毕业设计\\数据集\\T-drive Taxi Trajectories\\release\\taxi_10000";
    String csvFilePath = "D:\\毕业设计\\数据集\\T-drive Taxi Trajectories\\release\\taxi_10000.csv";
    // 遍历文件夹下所有txt文件
    // 创建CSV写入器
    try (PrintWriter writer = new PrintWriter(new FileWriter(csvFilePath))) {
      // 遍历文件夹下所有txt文件
      File folder = new File(path);
      File[] files = folder.listFiles();
      for (File file : files) {
        if (file.isFile() && file.getName().endsWith(".txt")) {
          // 读取txt文件内容并写入CSV
          String content = readFileContent(file);
          writer.print(content);
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  // 读取文件内容
  private static String readFileContent(File file) throws IOException {
    StringBuilder contentBuilder = new StringBuilder();
    try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
      String line;
      while ((line = reader.readLine()) != null) {
        contentBuilder.append(line).append("\n");
      }
    }
    return contentBuilder.toString();
  }
}
