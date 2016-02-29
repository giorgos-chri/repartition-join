package join;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;


public class IRJoin {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		
		if(args.length != 2){
			System.err.println("Wrong number of arguments..");
			System.err.println("Required 2 arguments for this Map Reduce program!!!");
			System.exit(2);
		}
				
		Job job = new Job(conf, "Improved Join");
		
		
		job.setJarByClass(IRJoin.class);
		
		Scanner in = new Scanner(System.in);
		//Asks from the user to give the number of reducers
		//This option is given so that can be easier to determine the 
		//proper execution of the program.
		System.out.println("Give the number of reducers:   ");
		int numOfReducers = in.nextInt();
		if(numOfReducers < 0){
			System.err.println("The number of reducers cannot be negative!");
			System.exit(3);
		}
		in.close();
		
		job.setNumReduceTasks(numOfReducers);
		//Set mapper reducer and custom shuffle and sort classes
		job.setMapperClass(MapJoin.class);
		job.setReducerClass(ReducerJoin.class);
		job.setPartitionerClass(CustomPartitioner.class);
		job.setSortComparatorClass(CustomSortComparator.class);
		job.setGroupingComparatorClass(GroupComparatorJoin.class);
		
		
		job.setOutputKeyClass(CompositeKey.class);
		job.setOutputValueClass(Text.class);
		
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		//If we have more than 1 reducers save the files produced by each reducer
		//in a file named by the output file name + _temp.
		if(numOfReducers > 1){
			FileOutputFormat.setOutputPath(job, new Path(args[1] + "_temp"));
		}
		//Else we will have only one reducer so we will only one output file
		//and we store it in the given output file.
		else
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		int done = job.waitForCompletion(true) ? 0 : 1;
		
		//If we have more than one reducers we must join the temp output files
		//in one last file
		//If we have only one reducer the job is completed.
		if(done == 0 && numOfReducers > 1){
			//if(numOfReducers > 1){				
			
			Job job2 = new Job(conf, "Join Temp Files");
			
			job2.setJarByClass(IRJoin.class);
			
			job2.setMapperClass(MapSort.class);
			job2.setReducerClass(ReduceSort.class);
			
			job2.setOutputKeyClass(IntWritable.class);
			job2.setOutputValueClass(Text.class);
			
			//The input of the second job are the temp files produced by
			//the first map reduce phase
			FileInputFormat.addInputPath(job2, new Path(args[1]+ "_temp"));
			//We store the output of the merged files in the given output file.
			FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);
		}
		else System.exit(done);
	}
}
