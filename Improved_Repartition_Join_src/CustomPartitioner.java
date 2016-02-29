package join;

import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

public class CustomPartitioner extends Partitioner<CompositeKey, Text>{
	
	@Override 
	public int getPartition(CompositeKey key, Text value, int reducers){
		int userID = key.getUserID();
		//Because we know that the IDs of the user are between 0-9999
		//the module will give equal partition between the reducers.
		//Also we ensure that all the records (from all the file) of a user 
		//will be send to the same reducer.
		return userID%reducers;
	}
}
