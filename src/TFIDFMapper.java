import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class TFIDFMapper implements Mapper<Text, Text, Text, Text> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void map(Text key, Text value, OutputCollector<Text, Text> output,
			Reporter reporter) throws IOException {
		String term = "";
		if (key.toString().contains(",,,,,")) {
			String[] parts = key.toString().split(",,,,,");
			String fileName = parts[0];
			if (parts.length == 2) {
				term = key.toString().split(",,,,,")[1];
			} else {
				term = "";
			}
			String valueString = fileName + ",,," + value.toString();
			output.collect(new Text(term), new Text(valueString));
		} else {
			term = key.toString();
			String valueString = "" + value.toString();
			output.collect(new Text(term), new Text(valueString));
		}
	}
}
