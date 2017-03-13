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

//Mapper1 and Reducer1 calculate the total number times each hall has been under utilized


public class CourseSchedule6 {

	public static class Mapper1	extends Mapper<Object, Text, Text, Text>{
		
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split(",");
			
			if(data.length > 11 || 
			data[10].equals("0") || 
			data[10].equals("1") || 
			data[3].contains("2017") ||
			data[5].contains("Unknown"))
			{
				//Ignore				
			}
			else{	
				// the difference between enrolled and capacity for the subject 
				int diff = Integer.parseInt(data[10]) - Integer.parseInt(data[9]);
				if(diff > 15){	
					String[] hall = data[5].split("\\s+");

					//generate key
					String key_ec = hall[0];				

					//set the value to # of current students enrolled and capacity
										
					//pass to reducer
					word.set(key_ec);
					context.write(word, new Text("1"));
				}				
			}
		}
	}


	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum_underutilized = 0;
					
			for (Text val : values) {				
				sum_underutilized += Integer.parseInt(val.toString());
			}
						
			result.set("" + sum_underutilized);
			context.write(key, result);
		}
	}


	public static void main(String[] args) throws Exception {
		//String temp="Tepo";
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "map reduce 1");
		job1.setJarByClass(CourseSchedule6.class);
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
				
	}
}










