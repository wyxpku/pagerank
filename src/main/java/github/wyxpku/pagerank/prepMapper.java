package github.wyxpku.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class prepMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		String[] line = ivalue.toString().split("	");
		String src = line[0].substring(1, line[0].length() - 1);
		String trg = line[1].substring(1, line[1].length() - 1);
		context.write(new Text(src), new Text(trg));
	}

}
