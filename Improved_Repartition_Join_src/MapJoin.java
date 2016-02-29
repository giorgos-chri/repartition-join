package join;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//Mapper of the Improved Repartition Join

public class MapJoin extends Mapper<LongWritable, Text, CompositeKey, Text>{
	
	/*
	 * The key output of the Mapper will be a composite key consisting
	 * from the user id and the tag that is given to the records according
	 * the file they come from. This will be the key by which the two files will join.
	 * The value output of the Mapper will be the rest of each record.
	 */
	private CompositeKey ckey = new CompositeKey();
	private Text record = new Text();
	
	public void map(LongWritable key, Text value, Context context
			) throws IOException, InterruptedException{
		//Get the name of each file and stores it in filename variable
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		String filename = fileSplit.getPath().getName();
		
		/*Checks if the filename equal to users.csv or transaction.csv
		 * If it equals users.csv we add a L tag to the record
		 * If it equals transactions.csv we add a R tag to the record 
		 * Else the file name is not correct so we print an error
		 */
		
		if(filename.equals("users.csv")){
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String[] parts = itr.nextToken().split(",");
				//the first string of the users.csv file is the user id,
				//we added to the composite key.
				ckey.setUserID(parts[0]);
				//We add to the composite key the tag (R for records from users.csv)
				ckey.setTag("R");
				//The rest of the record
				record.set(parts[1]);
				context.write(ckey, record);
			}
		}
		else if(filename.equals("transactions.csv")){
			StringTokenizer itr = new StringTokenizer(value.toString());
			while(itr.hasMoreTokens()){
				String[] parts = itr.nextToken().split(",");
				//the second string of the transactions.csv file is the user id,
				//we added to the composite key.
				ckey.setUserID(parts[1]);		
				//We add to the composite key the tag (L for records from transactions.csv)
				ckey.setTag("L");
				//The rest of the record consists from the transaction id and the
				//transaction name.
				record.set(parts[0] + "," + parts[2]);
				context.write(ckey, record);
			}
		}
		else{
			System.err.println("Error: Wrong File Name");
			System.exit(3);
		}
	}
}
