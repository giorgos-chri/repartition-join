package join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;


//Gets all the records from all the temp files and sorts them.

public class ReduceSort extends Reducer<IntWritable, Text, IntWritable, Text>{
	
	//As key in the TreeMap we set the user id and as value the rest of the record.
	TreeMap<Integer, ArrayList<String>> sortData = new TreeMap<>();
	Text finalValue = new Text();
	
	public void reduce(IntWritable key, Iterable<Text> value, Context context
			)throws IOException, InterruptedException{
		//Iterate all the records an put them in the TreeMap
		for (Text val : value){
			StringTokenizer itr = new StringTokenizer(val.toString());
			while(itr.hasMoreTokens()){
				String[] parts = itr.nextToken().split(",");
				int nextKey = Integer.parseInt(parts[0]);
				String v = new String(parts[1] + "," + parts[2] + "," + parts[3]);
				if(sortData.containsKey(nextKey)){
					sortData.get(nextKey).add(v);
				}
				else{
					ArrayList<String> newList = new ArrayList<>();
					newList.add(v);
					sortData.put(nextKey, newList);
				}
			}
		}
		//Output the final output file.
		for(Integer k : sortData.keySet()){
			String finalKey = String.valueOf(k);
			for(String v : sortData.get(k)){
				finalValue.set(finalKey + "," + v);
				context.write(null, finalValue);		
			}
		}
	}
}
