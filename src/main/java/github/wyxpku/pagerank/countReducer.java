package github.wyxpku.pagerank;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wyx on 17-1-14.
 */
public class countReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // process values
        context.write(_key, new Text("1"));
    }
}
