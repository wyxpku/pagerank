package github.wyxpku.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by wyx on 17-1-14.
 */
public class count {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "count");
        job.setJarByClass(github.wyxpku.pagerank.count.class);
        // TODO: specify a mapper
        job.setMapperClass(countMapper.class);
        // TODO: specify a reducer
        job.setReducerClass(countReducer.class);

        // TODO: specify output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path("hdfs://10.2.209.191:9000/Input"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://10.2.209.191:9000/Count"));

        if (!job.waitForCompletion(true))
            return;
    }
}