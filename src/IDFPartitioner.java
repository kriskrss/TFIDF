import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class IDFPartitioner implements Partitioner<Text, IntWritable> {

	@Override
	public void configure(JobConf arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public int getPartition(Text arg0, IntWritable arg1, int arg2) {
		// TODO Auto-generated method stub
		return 0;
	}

}
