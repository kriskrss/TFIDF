import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class TFIDFReducer implements Reducer<Text, Text, Text, DoubleWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void close() throws IOException {
		// TODO Auto-generated method stub

	}

	@Override
	public void reduce(Text term, Iterator<Text> values,
			OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		HashMap<String, Double> tfMap = new HashMap<String, Double>();
		double idf = 0;
		while (values.hasNext()) {
			String value = values.next().toString();
			if (!value.contains(",,,")) {
				idf = Double.parseDouble(value);
			} else {
				String fileName = value.split(",,,")[0];
				Double tf = Double.parseDouble(value.split(",,,")[1]);
				tfMap.put(fileName + "," + term.toString(), tf);
			}
		}
		Iterator<String> it = tfMap.keySet().iterator();
		while (it.hasNext()) {
			String k = it.next();
			System.out.println(k);
			output.collect(new Text(k), new DoubleWritable(idf * tfMap.get(k)));
		}
	}
}
