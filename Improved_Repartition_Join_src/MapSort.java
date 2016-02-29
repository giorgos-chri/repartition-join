package join;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

//Sends all the joined records to 1 reducer so that will be sorted automatically
//in the shuffle and sort phase. To achieve that we set as output key the same number
//to all the outputs.

//NOTE: This map reduce phase is only necessary if we have more than 1 reducers!!!

public class MapSort extends Mapper<Object, Text, IntWritable, Text>{	
	
	public void map(Object key, Text value, Context context
			) throws IOException, InterruptedException{
		
		IntWritable reducer = new IntWritable(1);
		context.write(reducer, value);
	}
}
