import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IDFMapper implements Mapper<Text, Text, Text, Text> {
	JobConf conf;

	@Override
	public void configure(JobConf arg0) {
		conf = arg0;
	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void map(Text fileName, Text line,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String[] words = line.toString().split("\\s+");
		System.out.println(fileName + "," + line);
		for (int i = 0; i < words.length; i++) {
			output.collect(new Text(words[i]), new Text(fileName));
		}
	}

}
