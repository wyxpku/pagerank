package github.wyxpku.pagerank;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wyx on 17-1-15.
 */
public class sortMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
    public void map(LongWritable ikey, Text ivalue, Context context) throws IOException, InterruptedException {
        String line = ivalue.toString();
        String[] splited_line = line.split("\t");
        if (splited_line.length == 2) {
            String src = splited_line[0];
            String[] info = splited_line[1].split(";");
            double rank = Double.parseDouble(info[0]);
            context.write(new DoubleWritable(rank), new Text(src));
        }
    }

}
