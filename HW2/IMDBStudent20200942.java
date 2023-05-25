//hadoop jar build/hadoop-project.jar IMDBStudent20200942 HW2_input output 10

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapred.lib.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
public class IMDBStudent20200942
{
	public static class RateComparator implements Comparator<Integer> {
		public int compare(Integer o1, Integer o2) {
			if ( o1 > o2 ) return 1;
			if ( o1 < o2 ) return -1;
			return 0;
		}
	}


	public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		boolean fileMovie = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
		
			Text outputKey = new Text();
			Text outputValue = new Text();
			StringTokenizer itr = new StringTokenizer(value.toString(), "::");
			String joinValue;
			String movieID;
			
			if(fileMovie) {
				movieID = itr.nextToken();
				String movieTitle = itr.nextToken();
				String token ="";
				while (itr.hasMoreTokens()){ 
					token = itr.nextToken();
				}
				
				StringTokenizer genre = new StringTokenizer(token, "|");
				int flag = 0;
				while (genre.hasMoreTokens()){ 
					if(genre.nextToken().equals("Fantasy")) {
						flag = 1;
						break;
					}
				}
				
				if (flag == 0 )
					joinValue = "N";
				else
					joinValue = "M=" + movieTitle; //Fantasy 있으면 M 아니면 N
			}
			
			else  
			{
				String userID = itr.nextToken();
				movieID = itr.nextToken();
				String movieRating = itr.nextToken();
				joinValue = "R=" + movieRating;
			}
			
			outputKey.set(movieID);
			outputValue.set(joinValue);
			context.write(outputKey, outputValue);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException
		{
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			if ( filename.indexOf( "ratings.dat" ) != -1 ) fileMovie = false;
			else fileMovie = true;
		}
	}
	
	public static class ReduceSideJoinReducer extends Reducer<Text,Text,Text,Text>
	{	
		private PriorityQueue<Integer> queue ;
		private Comparator<Integer> comp = new RateComparator();
		private int topK;
		Text reduce_key = new Text();
		Text reduce_result = new Text();
			
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			int flag = 0;
			String title = "";
			float rate = 0;
			
			for (Text val : values) {
				String file_type;
				StringTokenizer itr = new StringTokenizer(val.toString(), "=");
				file_type = itr.nextToken();
				if( file_type.equals( "N" ) ) { 
					flag = 1; break; 
				}
				
				if( file_type.equals( "M" ) )  {
					title = itr.nextToken();
				}
				else  { //topK 구하기
					int num = Integer.parseInt( itr.nextToken() );
					int head;
					if(queue.size() != 0)
						head = (int) queue.peek();
					else
						head = 0;
						
					if ( queue.size() < topK || head < num ) {
						queue.add( num );
						if( queue.size() > topK ) queue.remove();
					}
				}
			}
			
			int sum = 0;
			int size = queue.size();
			
			if (flag ==  1) {
				while( queue.size() != 0 ) {
					int n = queue.remove();
				}
				return;
			}
			else {
				//System.out.print(queue); System.out.print(title + "\n");
				while( queue.size() != 0 ) {
					int n = queue.remove();
					sum+=n;
				}
			}
			
			rate = (float)sum/size;
			
			reduce_key.set(title);
			reduce_result.set(rate + "");
			context.write(reduce_key, reduce_result);
				
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Integer>(topK , comp);
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 3)
		{
			System.err.println("Usage: IMDBStudent20200942 <in> <out>");
			System.exit(2);
		}
		int topK = Integer.parseInt( otherArgs[2] );
		conf.setInt("topK", topK);
		
		Job job = new Job(conf, "IMDBStudent20200942");
		job.setJarByClass(IMDBStudent20200942.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
