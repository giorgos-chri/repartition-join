package join;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

//Reducer of the Improved Repartition Join

public class ReducerJoin extends Reducer<CompositeKey, Text, Text, Text>{
	
	/*
	 * We create one HashMap where we will stored the data computed
	 * be the users.csv file (R Tagged). We store the R tagged records 
	 * and not the L tagged records because the users are fewer than 
	 * the transaction.
	 * As key to the HashMap we have the user ID (we get it from the composite key)
	 * and as value the rest of the record.
	 */
	
	private HashMap<Integer, String> RBuffer = new HashMap<>();
	private Text joinedValue = new Text();
	
	public void reduce(CompositeKey key, Iterable<Text> value, Context context
			)throws IOException, InterruptedException{
		
		for(Text val : value){
			//When we find a R tagged record we add it to the Buffer
			if(key.getTag().compareTo("R") == 0){
				RBuffer.put(key.getUserID(), val.toString());
			}
			//When we find a L tagged record we search the Buffer and 
			//create the join record.
			//NOTE: We are sure that the R tagged record will be inside the 
			//Buffer because we have set all the R tags before the L tags
			else if(key.getTag().compareTo("L") == 0){
				StringTokenizer itr = new StringTokenizer(val.toString());
				while(itr.hasMoreTokens()){
					String[] parts = itr.nextToken().split(",");
					joinedValue.set(key.getUserID() + "," 
								+ RBuffer.get(key.getUserID()) + "," 
								+ parts[0] + "," + parts[1]);
					context.write(null, joinedValue);
				}
			}
		}		
	}
}
