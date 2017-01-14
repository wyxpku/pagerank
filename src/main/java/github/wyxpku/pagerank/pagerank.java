package github.wyxpku.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * Created by wyx on 17-1-14.
 */
public class pagerank {
    public static enum counter {
        Map, num
    };
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem.setDefaultUri(conf, new URI("hdfs://10.2.209.191:9000"));
        for (int i = 0; i < 100; i++) {
            System.out.println("pagerank: loop " + i);
            Job job = Job.getInstance(conf, "pagerank-"+i);
            job.setJarByClass(github.wyxpku.pagerank.pagerank.class);
            // TODO: specify a mapper
            job.setMapperClass(prMapper.class);
            // TODO: specify a reducer
            job.setReducerClass(prReducer.class);

            // TODO: specify output types
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(Text.class);

            // TODO: specify input and output DIRECTORIES (not files)
//            FileInputFormat.setInputPaths(job, new Path("hdfs://10.2.209.191:9000/Input"));
//            FileOutputFormat.setOutputPath(job, new Path("hdfs://10.2.209.191:9000/prepOutput"));
            if (i == 0)
                FileInputFormat.setInputPaths(job, new Path("/prepOutput"));
            else
                FileInputFormat.setInputPaths(job, new Path("/prOutput" + (i - 1)));
            FileOutputFormat.setOutputPath(job, new Path("/prOutput" + i));
            job.waitForCompletion(true);
            Counters counter = job.getCounters();
            int count = (int) counter.findCounter(pagerank.counter.num).getValue();
            System.out.println("count: " + count);
            if (count < 283000)
                counter.findCounter(pagerank.counter.num).increment(0 - count);
            else
                break;
        }
    }
}