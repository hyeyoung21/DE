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
	public static class Movie {
		double rate;
		String title;
		
		Movie(double rate, String title) {
			this.rate = rate;
			this.title = title;
		}
		
		public String toString()
		{
			return title + ": " + rate;
		}
	}
	
	public static class MovieComparator implements Comparator<Movie> {
		public int compare(Movie x, Movie y) {
			if ( x.rate > y.rate ) return 1;
			if ( x.rate < y.rate ) return -1;
			return 0;
		}
	}
	
	public static void insertRate(PriorityQueue queue, int topK, double rate, String title) {
		Movie head = (Movie) queue.peek();
			
		if ( queue.size() < topK || head.rate < rate ) {
			Movie movie = new Movie(rate, title);
			queue.add( movie );
			if( queue.size() > topK ) queue.remove();
		}
	}
	
	public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, Text>
	{
		boolean fileMovie = true;
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
		
			Text outputKey = new Text();
			Text outputValue = new Text();
			String[] itr = value.toString().split("::");
			String joinValue;
			String movieID;
			
			if(fileMovie) {
				movieID = itr[0];
				String movieTitle = itr[1];
				StringTokenizer genre = new StringTokenizer(itr[2], "|");
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
				String userID = itr[0];
				movieID = itr[1];
				String movieRating = itr[2];
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
	
	public static class ReduceSideJoinReducer extends Reducer<Text,Text,Text,DoubleWritable>
	{	
		private PriorityQueue<Movie> queue ;
		private Comparator<Movie> comp = new MovieComparator();
		private int topK;
		Text reduce_key = new Text();
		DoubleWritable reduce_result = new DoubleWritable();

			
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException  {
			int sum = 0; //rating 총합
			int size = 0; //rating 갯수
			String title = "";
			double rate = 0;
			ArrayList<Integer> buffer = new ArrayList<Integer>();
			
			
			for (Text val : values) {
				String file_type;
				StringTokenizer itr = new StringTokenizer(val.toString(), "=");
				file_type = itr.nextToken();
				if( file_type.equals( "N" ) ) return; 
				
				if( file_type.equals( "M" ) )  {
					title = itr.nextToken();
				}
				else  { 
					int num = Integer.parseInt( itr.nextToken() );
					sum += num;
					size++;
					//buffer.add(num);
				}
			}
			rate = (double)sum/size;
			
			//System.out.println(buffer + "  " +title + ": " + rate );
			insertRate(queue, topK, rate, title);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<Movie>(topK, comp);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			//System.out.println(queue);
			while( !queue.isEmpty()) {
				Movie movie = (Movie) queue.remove();
				
				reduce_key.set(movie.title);
				reduce_result.set(movie.rate);
				context.write(reduce_key, reduce_result);
			}
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
