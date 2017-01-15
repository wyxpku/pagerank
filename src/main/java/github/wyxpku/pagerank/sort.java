package github.wyxpku.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by wyx on 17-1-15.
 */
public class sort {



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "sort");
        job.setJarByClass(github.wyxpku.pagerank.sort.class);
        // TODO: specify a mapper
        job.setMapperClass(github.wyxpku.pagerank.sortMapper.class);
        // TODO: specify a reducer
        job.setReducerClass(github.wyxpku.pagerank.sortReducer.class);

        // TODO: specify output types
        job.setOutputKeyClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/prOutput45"));
        FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/sorted"));

        if (!job.waitForCompletion(true))
            return;
    }
}
