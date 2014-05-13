import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class IDFReducer implements Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text word, Iterator<Text> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		HashSet<String> fileSet = new HashSet<String>();
		while (values.hasNext()) {
			fileSet.add(values.next().toString());
		}
		double num = fileSet.size() + 0.0;
		double idf = 3 / num;
		idf = Math.log(idf);
		output.collect(word, new DoubleWritable(idf));
	}

}
