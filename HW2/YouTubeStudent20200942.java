//hadoop jar build/hadoop-project.jar YouTubeStudent20200942 HW2_input output 10

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
public class YouTubeStudent20200942
{
	public static class YouTube {
		double rate;
		String title;
		
		YouTube(double rate, String title) {
			this.rate = rate;
			this.title = title;
		}
		
		public String toString()
		{
			return title + ": " + rate;
		}
	}
	
	public static class YouTubeComparator implements Comparator<YouTube> {
		public int compare(YouTube x, YouTube y) {
			if ( x.rate > y.rate ) return 1;
			if ( x.rate < y.rate ) return -1;
			return 0;
		}
	}
	
	public static void insertRate(PriorityQueue queue, int topK, double rate, String title) {
		YouTube head = (YouTube) queue.peek();
			
		if ( queue.size() < topK || head.rate < rate ) {
			YouTube yt = new YouTube(rate, title);
			queue.add( yt );
			if( queue.size() > topK ) queue.remove();
		}
	}
	
	public static class ReduceSideJoinMapper extends Mapper<Object, Text, Text, DoubleWritable>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
		
			Text outputKey = new Text();
			DoubleWritable outputValue = new DoubleWritable();
			String[] itr = value.toString().split("\\|");

			String genre = itr[3];
			double rate = Double.parseDouble(itr[6]);
			
			outputKey.set(genre);
			outputValue.set(rate);
			context.write(outputKey, outputValue);
		}
	}
	
	public static class ReduceSideJoinReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>
	{	
		private PriorityQueue<YouTube> queue ;
		private Comparator<YouTube> comp = new YouTubeComparator();
		private int topK;
		Text reduce_key = new Text();
		DoubleWritable reduce_result = new DoubleWritable();

			
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException  {
			double sum = 0; //rating 총합
			int size = 0; //rating 갯수
			String title = key.toString();
			double rate = 0;
			ArrayList<Double> buffer = new ArrayList<Double>();
			
			for (DoubleWritable val : values) {
				double num = val.get();
				sum += num;
				size++;
				buffer.add(num);
			}
			rate = (double)sum/size;
			
			//System.out.println("rate = " + rate + " sum= " + sum + " size= " + size + " title= " + title);
			insertRate(queue, topK, rate, title);
		}
		
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topK = conf.getInt("topK", -1);
			queue = new PriorityQueue<YouTube>(topK, comp);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			System.out.println(queue);
			while( !queue.isEmpty()) {
				YouTube yt = (YouTube) queue.remove();
				
				reduce_key.set(yt.title);
				reduce_result.set(yt.rate);
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
		
		Job job = new Job(conf, "YouTubeStudent20200942");
		job.setJarByClass(YouTubeStudent20200942.class);
		job.setMapperClass(ReduceSideJoinMapper.class);
		job.setReducerClass(ReduceSideJoinReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
