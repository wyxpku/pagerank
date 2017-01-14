package github.wyxpku.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class prepReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		String content = "1.0;";
		for (Text val : values) {
			content += (";" + val.toString());
		}
		context.write(_key, new Text(content));
	}
}
