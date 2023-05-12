import java.io.IOException;
import java.util.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class UBERStudent20200942 
{
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text value = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			//분류별로 나누기?
			//ex B02512,1/1/2018,190,1132
			String arr[] = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			
			String baseNumber = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			int activeVehicles = Integer.parseInt(itr.nextToken().trim());
			int trips = Integer.parseInt(itr.nextToken().trim());
			
			
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");		
				Date dateObj = dateFormat.parse(date);
			
				Calendar cal = Calendar.getInstance() ;
				cal.setTime(dateObj);
					     	
				int day = cal.get(Calendar.DAY_OF_WEEK) ;
				String dayOfWeek = arr[day];
				
				
				word.set(baseNumber + "," + dayOfWeek);
				value.set(one);	
		     	}
		     	catch(Exception e) { 
		     		
		     	}


			context.write(word, value);
		}	
	}

	public static class WordCountReducer extends Reducer<Text,Text,Text,Text> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
		{
			//같은 차량 같은 요일끼리 묶기
			
			int vehiclesSum = 0;
			int tripsSum = 0;
			
			for (Text val : value) 
			{
				StringTokenizer itr = new StringTokenizer(value.toString(), ",");
				vehiclesSum += Integer.parseInt(itr.nextToken().trim());
				tripsSum += Integer.parseInt(itr.nextToken().trim());
			}
			
			result.set(vehiclesSum + "," + tripsSum);
			context.write(key, result);	
		}
	}

	public static void main(String[] args) throws Exception 
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) 
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "UBERStudent20200942");
		job.setJarByClass(UBERStudent20200942.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}

