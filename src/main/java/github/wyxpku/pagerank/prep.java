package github.wyxpku.pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class prep {
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "prep");
		job.setJarByClass(github.wyxpku.pagerank.prep.class);
		// TODO: specify a mapper
		job.setMapperClass(prepMapper.class);
		// TODO: specify a reducer
		job.setReducerClass(prepReducer.class);

		// TODO: specify output types
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// TODO: specify input and output DIRECTORIES (not files)
		FileInputFormat.setInputPaths(job, new Path("hdfs://10.2.209.191:9000/Input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.2.209.191:9000/prepOutput"));

		if (!job.waitForCompletion(true))
			return;
	}
}
