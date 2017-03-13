import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* 
Group Members : Ranganath Sundar(r26@buffalo.edu) - Person number = 50169271
   		Shalmalee Vishwasrao(shalmale@buffalo.edu) - Person number = 50169915
*/

//Mapper1 and Reducer1 calculates enrollemnt for combination of hall and time
//Mapper2 and Reducer2 calculate number of halls which have 0 enrollment for particular time durations

public class CourseSchedule4 {

	public static class Mapper1	extends Mapper<Object, Text, Text, Text>{
		private final static Text enrolled_val = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split(",");
		
			if(data.length > 11 || 
			   data[5].contains("Unknown") || 
			   data[5].contains("Arr Arr") || 
			   data[7].contains("Unknown") ||
			   data[7].contains("Before 8:00AM") ||
			   data[7].contains("10:00PM and Later") )
			{
				//Ignore				
			}
			else{				
				String[] hall = data[5].split("\\s+");

				//generate key
				String key_ec = hall[0] + "_" + data[7];				

				//set the value to # of current students enrolled and capacity
				int enrolled = Integer.parseInt(data[9]);
				enrolled_val.set("" + enrolled);
				
				//both to be passed to reducer
				word.set(key_ec);
				context.write(word, enrolled_val);				
			}
		}
	}


	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum_enrolled = 0;
			//int sum_capacity = 0;
					
			for (Text val : values) {
				sum_enrolled += Integer.parseInt(val.toString());
			}
						
			result.set("" + sum_enrolled);
			context.write(key, result);
		}
	}
	
	//find the halls which have 0 students enrolled for at any point of time in the day
	public static class Mapper2	extends Mapper<Object, Text, Text, Text>{
		
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split("\\t");
			
			if(Integer.parseInt(data[1]) != 0){
				//Ignore
			}
			else{						
				String key1 = data[0].trim().split("_")[1];

				word.set(key1);
				context.write(word, new Text("" + 1));
			}
		}
	}


	//counts the number of halls having 0 enrollment at given time of the day
	public static class Reducer2 extends Reducer<Text,Text,Text,Text> {

		Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
									
			int count = 0;
			
			for (Text val : values) {
				count += Integer.parseInt(val.toString());
			}
			result.set(new Text("" + count));
			context.write(key, result);			
		}
	}

	public static void main(String[] args) throws Exception {
		//String temp="Tepo";
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "map reduce 1");
		job1.setJarByClass(CourseSchedule4.class);
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);// ? 0 : 1;
		
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "map reduce 2");
		job2.setJarByClass(CourseSchedule4.class);
		job2.setMapperClass(Mapper2.class);
		job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
}


