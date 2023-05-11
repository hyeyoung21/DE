import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20200942 {

	public static class Map extends Mapper<LongWritable, Text, Text, LongWritable> {
	private final LongWritable one = new LongWritable(1);
	private Text genre = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//장르별로 나누기
			//ex 1::Toy Story (1995)::Animation|Children's|Comedy
			
			StringTokenizer itr = new StringTokenizer(value.toString(), "::");
			
			String[] arr = new String[4]; // 결과 배열
			int idx = 0;
			while (itr.hasMoreTokens()){
			    arr[idx] = itr.nextToken(); // 배열에 한 토큰씩 담기
			    idx++;
			}
			
			itr = new StringTokenizer(arr[2], "|");
			
			
			while (itr.hasMoreTokens()) 
			{
				genre.set(itr.nextToken());
				context.write(genre, one);
			}
		
		}
	}
	
	public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	private LongWritable result = new LongWritable();
	
		public void reduce(Text key, Iterable<LongWritable> value, Context context) throws IOException, InterruptedException {
			//카운트 하기
			int sum = 0;
			
			for (LongWritable val : value) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: IMDB <in> <out>");
			System.exit(2);
		}
		    	
		Job job = new Job(conf, "IMDBStudent20200942");
		job.setJarByClass(IMDBStudent20200942.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setCombinerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		job.waitForCompletion(true);
	}
}

