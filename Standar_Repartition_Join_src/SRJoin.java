package join;

/*
* Πρώτη εργασία στα Κατανεμημένα & Διαδικτυακά Συστήματα
* Στοιχεία Φοιτητών:
*
* Χριστάκης Γεώργιος 1559
* Γεωργιάδης Χρήστος Ανέστης 2459
* Χαντζής Ιωάννης 1551
*/


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

public class SRJoin {
	
	public static class MapJoin extends Mapper<Object, Text, IntWritable, Text>{
		
		/*
		 * The key output of the Mapper will be a user id for both the files
		 * and it will the key by which the two files will join.
		 * The value output of the Mapper will be the rest of each record tagged
		 * accordingly the file it is stored.  
		 */
		private IntWritable join_key = new IntWritable();
		private Text tagged_record = new Text();
		
		public void map(Object key, Text value, Context context
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
					//the join_key is the first string (user id) of the users.csv file
					join_key.set(Integer.parseInt(parts[0]));
					//the tagget_record consists from the second string (user name) tagged with L 
					tagged_record.set("L," + parts[1]);
					context.write(join_key, tagged_record);
				}
			}
			else if(filename.equals("transactions.csv")){
				StringTokenizer itr = new StringTokenizer(value.toString());
				while(itr.hasMoreTokens()){
					String[] parts = itr.nextToken().split(",");
					//the join_key is the second string (user id) of the transactions.csv file
					join_key.set(Integer.parseInt(parts[1]));
					/* the tagget_record consists from the first string (transaction id)
					 * the third string (transaction name) and the R tag
					*/
					tagged_record.set("R," + parts[0] + "," + parts[2]);
					context.write(join_key, tagged_record);
				}
			}
			else{
				System.err.println("Error: Wrong File Name");
				System.exit(3);
			}
		}
	}
	
	
	public static class JoinReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
		/*
		 * We create two HashMap where the data will be stored depending their tag
		 * one for L tagged and one for the R tagged.
		 * The key of the two HashMap will be the key by which the join will take place.
		 */
		
		private HashMap<Integer, String> LBuffer = new HashMap<>();
		private HashMap<Integer, ArrayList<String>> RBuffer = new HashMap<>();
		
		
		private IntWritable finalKey = new IntWritable();
		private Text joinedValue = new Text();
		
		public void reduce(IntWritable key, Iterable<Text> value, Context context
				)throws IOException, InterruptedException{
			for (Text val : value){
				int nextKey = key.get();
				StringTokenizer itr = new StringTokenizer(val.toString());
				while(itr.hasMoreTokens()){
					String[] parts = itr.nextToken().split(",");
					//If L is the tag of the String we place it in LBuffer
					if (parts[0].equals("L")){
						LBuffer.clear();
						LBuffer.put(nextKey, parts[1]);
					}
					//Else if R is the tag of the String we place it in RBuffer
					else if (parts[0].equals("R")){
						if(RBuffer.containsKey(nextKey)){
							ArrayList<String> temp = RBuffer.get(nextKey); 
							temp.add(parts[1] + "," + parts[2]);
						}
						else {
							RBuffer.clear();
							ArrayList<String> newList = new ArrayList<>();
							newList.add(parts[1] + "," + parts[2]);
							RBuffer.put(nextKey, newList);
						}
					}
				}
			}
			
			/* For each LBuffer key we find the value of RBuffer for that key
			 * and we join the two values 
			 */
			for(Integer BL : LBuffer.keySet()){
				ArrayList<String> rBufferList = RBuffer.get(BL);
				for(String rBufferValue : rBufferList){
					joinedValue.set(BL.toString() + "," + LBuffer.get(BL) + "," + rBufferValue);
					finalKey.set(BL);
					context.write(null, joinedValue);
				}	
			}
		}
		
	}	
	
	//Main Class join.SRJoin
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		if(args.length != 2){
			System.err.println("Error");
			System.exit(2);
		}
		Job job = new Job(conf, "Join Files");
		job.setJarByClass(SRJoin.class);
		job.setMapperClass(MapJoin.class);
		job.setReducerClass(JoinReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}

