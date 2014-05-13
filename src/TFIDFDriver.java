import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDFDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		JobConf tfConf = new JobConf(getConf(), TFIDFDriver.class);
		tfConf.setJobName("TF");
		tfConf.setOutputKeyClass(Text.class);
		tfConf.setJarByClass(TFIDFDriver.class);
		tfConf.setMapOutputValueClass(IntWritable.class);
		tfConf.setMapperClass(TFMapper.class);
		tfConf.setReducerClass(TFReducer.class);
		tfConf.setInputFormat(DocumentInputFormat.class);
		FileInputFormat.addInputPath(tfConf, new Path(args[0]));
		FileOutputFormat.setOutputPath(tfConf, new Path(args[1]));
		JobClient.runJob(tfConf);
		JobConf idfconf = new JobConf(getConf(), TFIDFDriver.class);
		idfconf.setJobName("IDF");
		idfconf.setOutputKeyClass(Text.class);
		idfconf.setJarByClass(TFIDFDriver.class);
		idfconf.setMapperClass(IDFMapper.class);
		idfconf.setReducerClass(IDFReducer.class);
		idfconf.setInputFormat(DocumentInputFormat.class);
		FileInputFormat.addInputPath(idfconf, new Path(args[0]));
		FileOutputFormat.setOutputPath(idfconf, new Path(args[2]));
		JobClient.runJob(idfconf);
		JobConf tfidfconf = new JobConf(getConf(), TFIDFDriver.class);
		tfidfconf.setJobName("TF-IDF");
		tfidfconf.setOutputKeyClass(Text.class);
		tfidfconf.setJarByClass(TFIDFDriver.class);
		tfidfconf.setMapperClass(TFIDFMapper.class);
		tfidfconf.setReducerClass(TFIDFReducer.class);
		tfidfconf.setInputFormat(KeyValueTextInputFormat.class);
		FileInputFormat.addInputPaths(tfidfconf, args[1] + "," + args[2]);
		FileOutputFormat.setOutputPath(tfidfconf, new Path(args[3]));
		JobClient.runJob(tfidfconf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TFIDFDriver(), args);
		System.exit(res);
	}

}
