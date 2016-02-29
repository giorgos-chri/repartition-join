package join;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

//The composite key used for the sort of the data.

public class CompositeKey implements WritableComparable<CompositeKey>{
	
	//The 2 values of the composite key
	//User ID and the tag (R or L) of the record.
	private int id;
	private String bufferTag;
	
	public CompositeKey(){
		
	}
	//Initialize the composite key
	public CompositeKey(String userID, String bufferTag){
		this.id = Integer.parseInt(userID);
		this.bufferTag = bufferTag;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeVInt(out, id);
		WritableUtils.writeString(out, bufferTag);		
	}
	
	
	@Override
	public void readFields(DataInput in) throws IOException {
		this.id = WritableUtils.readVInt(in);
		this.bufferTag = WritableUtils.readString(in);		
	}

	
	@Override
	public int compareTo(CompositeKey o) {		
		if(this.id == o.id){
			return -this.bufferTag.compareTo(o.bufferTag);
		}
		return this.id < o.id ? -1 : 1;
	}
	
	
	public void setUserID(String id){
		this.id = Integer.parseInt(id);
	}

	
	public int getUserID(){
		return id;
	}
	
	public void setTag(String tag){
		this.bufferTag = tag;
	}
	
	public String getTag(){
		return bufferTag;
	}	
}
