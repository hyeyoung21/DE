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
				joinValue = "M," + movieTitle;
			}
			
			else  
			{
				String userID = itr.nextToken();
				movieID = itr.nextToken();
				String movieRating = itr.nextToken();
				joinValue = "R," + movieRating;
			}
			
			System.out.println(movieID + " : " + joinValue);
			
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
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, 
		InterruptedException  {
			Text reduce_key = new Text();
			Text reduce_result = new Text();
			String title = "";
			String rate = "";
			
			for (Text val : values) {
				String file_type;
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				file_type = itr.nextToken();
				
				if( file_type.equals( "M" ) )  {
					title = itr.nextToken();
				}
				else  {
					rate = itr.nextToken();
				}
				
				System.out.println(val);
			}
			
			reduce_key.set(title);
			reduce_result.set(rate);
			context.write(reduce_key, reduce_result);
			
			
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
		
		Job job = new Job(conf, "IMDBStudent20200942");
		job.setJarByClass(IMDBStudent20200942.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		int topK = Integer.parseInt(otherArgs[2]);
		conf.setInt("topK", topK);
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
