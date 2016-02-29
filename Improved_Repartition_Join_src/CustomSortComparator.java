package join;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

//Gives the data in ascending order according the user IDs and
//in descending order according the tag of the record.
public class CustomSortComparator extends WritableComparator{

	protected CustomSortComparator() {
		super(CompositeKey.class, true);
	}
	@Override
	public int compare(WritableComparable w1, WritableComparable w2){
		CompositeKey ck1 = (CompositeKey) w1;
		CompositeKey ck2 = (CompositeKey) w2;
		//If the 2 records have same user id
		//The ordering will be according their tag. And because we want first
		//the R tagged records we want the result in descending order
		//(That's the reason for the -)
		if(ck1.getUserID() == ck2.getUserID()){
			return -ck1.getTag().compareTo(ck2.getTag());
		}
		//If the 2 records have different id the sort will be according to the 
		//user id in ascending order. (1 2 ....)
		return ck1.getUserID() < ck2.getUserID() ? -1 : 1;		
	}
}
