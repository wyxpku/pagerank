package github.wyxpku.pagerank;

/**
 * Created by wyx on 17-1-14.
 */

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class countMapper extends Mapper<LongWritable, Text, Text, Text> {
    String regex = "\"(\\d+)\"\t\"(\\d+)\"";
    private Pattern p = Pattern.compile(regex);
    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        Matcher m = p.matcher(ivalue.toString());
        if (m.find()) {
            context.write(new Text(m.group(1)), new Text("1"));
            context.write(new Text(m.group(2)), new Text("1"));
        }
    }

}
