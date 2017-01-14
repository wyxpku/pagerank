package github.wyxpku.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

import github.wyxpku.pagerank.pagerank.counter;

/**
 * Created by wyx on 17-1-14.
 */
public class prReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    public void reduce(LongWritable _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double srcpr = 0;
        ArrayList<String> ids = new ArrayList<String>();
        double pr = 0;

        // damping factor
        double q = 0.85;
        for (Text ivalue: values) {
            String curvalue = ivalue.toString();
            if (curvalue.charAt(0) == '@') {
                pr += Double.parseDouble(curvalue.substring(1));
            } else if (curvalue.charAt(0) == '#') {
                ids.add(curvalue.substring(1));
            } else if (curvalue.charAt(0) == '$') {
                srcpr = Double.parseDouble(curvalue.substring(1));
            }
        }
        pr = pr * q + (1 - q);
        if (Math.abs(srcpr - pr) < 0.1)
            context.getCounter(counter.num).increment(1);
        String content = "" + pr;
        for (String tmp: ids) {
            content += (";" + tmp);
        }
        context.write(_key, new Text(content));
    }
}
