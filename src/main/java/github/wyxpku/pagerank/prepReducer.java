package github.wyxpku.pagerank;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class prepReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text _key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		// process values
		String content = "1.0";
		Set tset = new HashSet<String>();
		Set fset = new HashSet<String>();
		for (Text val : values) {
			String tmp = val.toString();
			if (tmp.charAt(0) == 'T')
				tset.add(tmp.substring(1));
			else if (tmp.charAt(0) == 'F')
				fset.add(tmp.substring(1));
		}
		for (Iterator it = tset.iterator(); it.hasNext(); ){
			String curstr = it.next().toString();
			if (!curstr.equals(_key.toString()))
				content += (";T" + curstr);
		}
		for (Iterator it = fset.iterator(); it.hasNext(); ){
			String curstr = it.next().toString();
			if (!curstr.equals(_key.toString()))
				content += (";F" + curstr);
		}
		context.write(_key, new Text(content));
	}
}
