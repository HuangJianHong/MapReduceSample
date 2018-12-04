package customoutput;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MyOutputFormate extends FileOutputFormat<Text, NullWritable> {
    public RecordWriter getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        return new MyRecordWriter(taskAttemptContext);
    }
}

class MyRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream itStartOutPutStream;
    private FSDataOutputStream otherOutputStream;

    public MyRecordWriter(TaskAttemptContext job) throws IOException {
        // 1 获取文件系统
        FileSystem fileSystem = FileSystem.get(job.getConfiguration());

        // 2 创建输出文件路径
        Path itstartPath = new Path("F:\\Tmp\\output2\\itstart.txt");
        Path otherPath = new Path("F:\\Tmp\\output2\\other.txt");

        // 3 创建输出流
        itStartOutPutStream = fileSystem.create(itstartPath);
        otherOutputStream = fileSystem.create(otherPath);
    }

    public void write(Text text, NullWritable nullWritable) throws IOException {
        String data = text.toString();
        // 判断是否包含“itstar”输出到不同文件
        if (data.contains("itstar")) {
            itStartOutPutStream.write(data.getBytes());
        } else {
            otherOutputStream.write(data.getBytes());
        }
    }

    public void close(TaskAttemptContext taskAttemptContext) throws IOException {
        if (itStartOutPutStream != null) {
            itStartOutPutStream.close();
        }
        if (otherOutputStream != null) {
            otherOutputStream.close();
        }
    }


}

