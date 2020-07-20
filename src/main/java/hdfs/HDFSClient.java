package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;


public class HDFSClient {
    private FileSystem fs;

    @Before
    public void before() throws IOException, InterruptedException {
        fs = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "zero");
        System.out.println("Before !!!");
    }

    @Test
    public void put() throws IOException, InterruptedException {
        //获取一个HDFS的封装对象
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), configuration, "zero");


        //用这个对象操作！！！
        fileSystem.copyFromLocalFile(new Path("E:\\1.txt"), new Path("/"));//发送
//        fileSystem.copyToLocalFile();

        //关闭
        fileSystem.close();
    }

    @Test
    public void rename() throws IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9000"), new Configuration(), "zero");
        //操作
        fileSystem.rename(new Path("/text"), new Path("/text2"));

        fileSystem.close();

    }

    @Test
    public void delete() throws IOException{
        boolean delete = fs.delete(new Path("/1.txt"),true);
        if (delete){
            System.out.println("删除成功");
        }else{
            System.out.println("删除失败");
        }
    }

    @Test
    public void du(){
        fs.append();
    }

    @After
    public void after() throws IOException {
        System.out.println("After!!!");
        fs.close();
    }
}