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

public class IMDBStudent20201051
{
        public static class Map extends Mapper<Object, Text, Text, LongWritable>{
            private final static LongWritable one = new LongWritable(1);
            private Text word = new Text();

            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
                    StringTokenizer itr = new StringTokenizer(value.toString(), "::"); 
                    int id = Integer.parseInt( itr.nextToken().trim()); 
                    itr.nextToken(); 
                    itr.nextToken();

                    StringTokenizer tokenizer = new StringTokenizer(genre, "|"); 

                    while (tokenizer.hasMoreTokens()) {
                        String token = itr.nextToken().trim();
                        word.set(token);
                        context.write(word, one);
                    }
            }   
        }

        public static class Reduce extends Reducer<Text, LongWritable, Text, LongWritable>
        {
            private LongWritable sumWritable = new LongWritable();

            public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
                long sum = 0;

                for (LongWritable val : values) {
                    sum += val.get();
                }

                sumWritable.set(sum);
                context.write(key, sumWritable);
            }
        }

        public static void main(String[] args) throws Exception
        {
                Configuration conf = new Configuration();
                Job job = new Job(conf, "IMDBStudent20201051");

                job.setJarByClass(IMDBStudent20201051.class);
                job.setMapperClass(Map.class);
                job.setCombinerClass(Reduce.class);
                job.setReducerClass(Reduce.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(LongWritable.class);

                job.setInputFormatClass(TextInputFormat.class);
                job.setOutputFormatClass(TextOutputFormat.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
														FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
        

                job.waitForCompletion(true);
							}
}
