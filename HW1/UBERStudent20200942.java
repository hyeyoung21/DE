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

public class UBERStudent20200942 {

	public static class Map extends Mapper<Object, Text, Text, Text> {
		private Text name = new Text();
		private Text value = new Text();
	
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			//분류별로 나누기?
			//ex B02512,1/1/2018,190,1132
			String arr[] = {"MON", "TUE", "WED", "THR", "FRI", "SAT", "SUN"};
			
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			
			String baseNumber = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			String activeVehicles = itr.nextToken().trim();
			String trips = itr.nextToken().trim();
				
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
				Date dateObj = dateFormat.parse(date);
			
				Calendar cal = Calendar.getInstance() ;
				cal.setTime(dateObj);
				
				int day = cal.get(Calendar.DAY_OF_WEEK) ;
				String dayOfWeek = arr[day];
								
				name.set(baseNumber + "," + dayOfWeek);
				value.set(activeVehicles + "," + trips);
				
				context.write(name, value);
		     	}
		     	catch(Exception e) { 
		     		
		     	}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
	private Text result = new Text();
	
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			//같은 차량 같은 요일끼리 묶기
			int vehiclesSum = 0;
			int tripsSum = 0;
			
			
			for (Text val : value)
			{
				StringTokenizer itr = new StringTokenizer(val.toString(), ",");
				
				vehiclesSum += Integer.parseInt(itr.nextToken().trim());
				tripsSum += Integer.parseInt(itr.nextToken().trim());
			}
			
			result.set(vehiclesSum + "," + tripsSum);
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
		    	
		Job job = new Job(conf, "UBERStudent20200942");
		job.setJarByClass(UBERStudent20200942.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);


		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}

