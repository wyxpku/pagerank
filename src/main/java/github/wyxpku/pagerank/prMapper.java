package github.wyxpku.pagerank;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

            ArrayList<Long> tlist = new ArrayList<Long>();
            ArrayList<Long> flist = new ArrayList<Long>();
            for (int i = 1; i < splited.length; i++){
                String tmp = splited[i];
                if (tmp.charAt(0) == 'T') {
                    tlist.add(Long.parseLong(tmp.substring(1)));
                } else if (tmp.charAt(0) == 'F') {
                    flist.add(Long.parseLong(tmp.substring(1)));
                }
            }

            // size of tlist is zero, which means a black hole
            if (tlist.size() == 0) {
                for (Long ilong: flist) {
                    context.write(new LongWritable(ilong), new Text("@" + pr / flist.size()));
                }
            } else {
                for (Long ilong: tlist) {
                    context.write(new LongWritable(ilong), new Text("@" + pr / tlist.size()));
                }
            }

            context.write(new LongWritable(src), new Text("$" + pr));

            for (Long ilong: tlist) {
                context.write(new LongWritable(src), new Text("#T" + ilong));
            }
            for (Long ilong: flist) {
                context.write(new LongWritable(src), new Text("#F" + ilong));
            }
        }
    }
}
