package join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Gives the data as a list according the user ID.
public class GroupComparatorJoin extends WritableComparator{

	protected GroupComparatorJoin() {
		super(CompositeKey.class, true);
	}
	
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		CompositeKey ck1 = (CompositeKey) w1;
		CompositeKey ck2 = (CompositeKey) w2;
		
		return ck1.getUserID() < ck2.getUserID() ? -1 : 1;
	}

}
