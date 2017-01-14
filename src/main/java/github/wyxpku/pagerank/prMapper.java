package github.wyxpku.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

/**
 * Created by wyx on 17-1-14.
 */
public class prMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    String regex ="(\\d+)\\t(.*)";
    Pattern p = Pattern.compile(regex);
    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        String line = ivalue.toString();
        Matcher m = p.matcher(line);
        if (m.find()){
            long src = Long.parseLong(m.group(1));
            String rest = m.group(2);
            String[] splited = rest.split(";");
            if (splited.length == 0)
                return;
            double pr = Double.parseDouble(splited[0]);
            int count = splited.length - 1;
            for (int i = 1; i < splited.length; ++i){
                context.write(new LongWritable(Long.parseLong(splited[i])), new Text("@" + pr/count));
                context.write(new LongWritable(src), new Text("#" + splited[i]));
            }
            context.write(new LongWritable(src), new Text("$" + pr));
        }
    }
}
