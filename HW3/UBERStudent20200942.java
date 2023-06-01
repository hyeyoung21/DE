import scala.Tuple2;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;

import java.io.Serializable;
import java.util.*;
import org.apache.hadoop.io.*;
import java.util.regex.Pattern;

public final class UBERStudent20200942 {

    public static void main(String[] args) throws Exception {

        if (args.length < 2) {
            System.err.println("Usage: UBERStudent20200942 <in-file> <out-file>");
            System.exit(1);
        }

        SparkSession spark = SparkSession
            .builder()
            .appName("UBERStudent20200942")
            .getOrCreate();

        JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
		public Tuple2<String, String> call(String s) {
            		String arr[] = {"SUN", "MON", "TUE", "WED", "THR", "FRI", "SAT"};
			String name = "";
			String value = "";
			
			StringTokenizer itr = new StringTokenizer(s, ",");
			
			String baseNumber = itr.nextToken().trim();
			String date = itr.nextToken().trim();
			String activeVehicles = itr.nextToken().trim();
			String trips = itr.nextToken().trim();
				
			try {
				SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");
				Date dateObj = dateFormat.parse(date);
			
				Calendar cal = Calendar.getInstance() ;
				cal.setTime(dateObj);
				
				int day = cal.get(Calendar.DAY_OF_WEEK) ;
				String dayOfWeek = arr[day-1];
								
				name = baseNumber + "," + dayOfWeek;
				value = activeVehicles + "," + trips;
				
		     	}
		     	catch(Exception e) { 
		     		System.err.println("Exception");
		     	}
			
			return new Tuple2(name, value);
            }
        };
        JavaPairRDD<String, String> ones = lines.mapToPair(pf);

        Function2<String, String, String> f2 = new Function2<String, String, String>() {
            public String call(String x, String y) {	
		StringTokenizer xToken = new StringTokenizer(y, ",");
		StringTokenizer yToken = new StringTokenizer(x, ",");
		
		int vehiclesSum = Integer.parseInt(xToken.nextToken().trim()) + Integer.parseInt(yToken.nextToken().trim());
		int tripsSum = Integer.parseInt(xToken.nextToken().trim()) + Integer.parseInt(yToken.nextToken().trim());
		
		String result = tripsSum + "," + vehiclesSum;
		return result;
            }
        };
        JavaPairRDD<String, String> counts = ones.reduceByKey(f2);

        counts.saveAsTextFile(args[1]);
        spark.stop();
    }
}
