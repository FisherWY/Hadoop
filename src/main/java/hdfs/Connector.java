package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @Author Fisher
 * @Date 2020/2/9 10:29
 **/

public class Connector {

    // log4j日志记录器
    private static final Logger logger = LoggerFactory.getLogger("Hadoop logger");

    // hadoop home：本机Hadoop地址，需要根据自己电脑的地址修改
    private static final String HADOOP_HOME = "D:\\hadoop-3.2.1";

    // hadoop文件系统，默认无需修改
    private static final String FILE_SYSTEM = "fs.defaultFS";

    // HDFS地址，默认无需修改
    private static final String HDFS_ADDR = "hdfs://fisher.lazybone.xyz:9000";

    // HDFS用户名，暂时没派上用场
    private static final String USER_NAME = "dr.who";

    private Configuration conf = null;

    private FileSystem fs = null;

    /**
     * File System初始化
     * @throws IOException
     */
    public void init() throws IOException {
        System.setProperty("hadoop.home.dir", HADOOP_HOME);
        conf = new Configuration();
        conf.set(FILE_SYSTEM, HDFS_ADDR);

        fs = FileSystem.get(conf);
    }

    /**
     * 列出目录下的文件信息
     * @throws IOException
     */
    public void listFiles() throws IOException {
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(new Path("/"), true);

        logger.info("File       Size        Permission      Length");

        while (remoteIterator.hasNext()) {
            LocatedFileStatus fileStatus = remoteIterator.next();
            logger.info(fileStatus.getPath().getName() + "   " + fileStatus.getBlockSize() + "   " + fileStatus.getPermission() + "  " + fileStatus.getLen());
        }
    }

    /**
     * 将文件上传到服务器种
     * @param dest  目标地址
     * @param src   要上传的文件
     * @return  true表示为上传成功，失败则抛出IO错误
     * @throws IOException
     */
    public boolean saveFiles(String dest, String src) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        fs.copyFromLocalFile(srcPath, destPath);
        return true;
    }

    /**
     * 删除位于服务器上的文件/文件夹
     * @param src   要删除的文件/文件夹
     * @return  true表示为删除成功，失败则抛出IO错误
     * @throws IOException
     */
    public boolean deleteFiles(String src) throws IOException {
        Path srcPath = new Path(src);
        fs.delete(srcPath, true);
        return true;
    }

    /**
     * 下载位于服务器上的文件
     * @param dest  保存的位置
     * @param src   要下载的文件路径
     * @return  true表示为下载成功，失败则抛出IO错误
     * @throws IOException
     */
    public boolean downloadFiles(String dest, String src) throws IOException {
        Path srcPath = new Path(src);
        Path destPath = new Path(dest);
        fs.copyToLocalFile(srcPath, destPath);
        return true;
    }

    /**
     * 测试listFiles()
     * @param connector
     */
    private void test_listFiles(Connector connector) {
        try {
            connector.listFiles();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 下载文件测试
     * @param connector
     */
    private void test_downloadFiles(Connector connector) {
        String src = "/data/heart.csv";
        String dest = "D:\\hadoop-3.2.1\\data";
        try {
            if (connector.downloadFiles(dest, src)) {
                logger.info("下载成功");
            } else {
                logger.error("下载失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 上传文件测试
     * @param connector
     */
    private void test_uploadFiles(Connector connector) {
        String src = "D:\\hadoop-3.2.1\\data\\heart.csv";
        String dest = "/test/";
        try {
            if (connector.saveFiles(dest, src)) {
                logger.info("上传成功");
            } else {
                logger.error("下载失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件测试
     * @param connector
     */
    private void test_deleteFiles(Connector connector) {
        String src = "/test/heart.csv";
        try {
            if (connector.deleteFiles(src)) {
                logger.info("删除成功");
            } else {
                logger.error("删除失败");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取文件测试
     * @param connector
     */
    private void test_readFiles(Connector connector) {
        String src = "/test/setup.log";
        Path srcPath = new Path(src);
        try {
            FSDataInputStream in = connector.fs.open(srcPath);
            while (in.available() > 0) {
                String s = in.readUTF();
                logger.info(s);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 写入文件测试
     * @param connector
     */
    private void test_writeFiles(Connector connector) {
        String src = "/test/write.log";
        Path srcPath = new Path(src);
        try {
            FSDataOutputStream out = connector.fs.create(srcPath);
            out.writeUTF("Writing test");
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        Connector connector = new Connector();
        try {
            // hadoop fs初始化
            connector.init();
            // 读出文件列表测试
            // connector.test_listFiles(connector);
            // 下载文件测试
            // connector.test_downloadFiles(connector);
            // 上传文件测试
            // connector.test_uploadFiles(connector);
            // 删除文件测试
            // connector.test_deleteFiles(connector);
            // 读取文件流测试
            // connector.test_readFiles(connector);
            // 写入文件流测试
            // connector.test_writeFiles(connector);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
