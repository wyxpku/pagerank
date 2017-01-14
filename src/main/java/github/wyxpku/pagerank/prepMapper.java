package github.wyxpku.pagerank;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class prepMapper extends Mapper<LongWritable, Text, Text, Text> {
	String regex = "\"(\\d+)\"\t\"(\\d+)\"";
	private Pattern p = Pattern.compile(regex);
	public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
		Matcher m = p.matcher(ivalue.toString());
		if (m.find()) {
			String src = m.group(1);
			String tag = m.group(2);
			context.write(new Text(m.group(1)), new Text("T" + m.group(2)));
			context.write(new Text(m.group(2)), new Text("F" + m.group(1)));
		}
	}

}
