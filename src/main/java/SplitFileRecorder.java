import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;

/**
 * Created by harsh on 20/1/17.
 */
public class SplitFileRecorder {

    public static void main(String args[]) throws Exception{

        Configuration conf = new Configuration(true);
        JobConf jobConf = new JobConf(conf);
        try {

            FileInputFormat fileInputFormat = new TextInputFormat();
            ((TextInputFormat)fileInputFormat).configure(jobConf);
            fileInputFormat.addInputPaths(jobConf,"hdfs://localhost:54310/harsh/chi/sample_libsvm_data.txt,hdfs://localhost:54310/harsh/chi/filteredData");
            InputSplit split[] = fileInputFormat.getSplits(jobConf,2);
            RecordReader r[] = new RecordReader[split.length];
            for (int i=0 ; i<split.length;i++){
                System.out.println("This is split no. "+i);
                r[i]= fileInputFormat.getRecordReader(split[i],jobConf,Reporter.NULL);
                LongWritable key = new LongWritable();
                Text value = new Text();
                while (r[i].next(key, value))
                    System.out.println(key + " " + value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
