package github.wyxpku.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wyx on 17-1-15.
 */
public class sortReducer extends Reducer<DoubleWritable, Text, DoubleWritable, Text> {

    public void reduce(DoubleWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String content = "";
        boolean first = true;
        for (Text ctext: values) {
            if (first){
                content += ctext.toString();
                first = false;
            } else {
                content += (";" + ctext.toString());
            }
        }
        context.write(_key, new Text(content));
    }
}