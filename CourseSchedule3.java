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

//Mapper1 and Reducer1 calculate the total capacity per hall per year
//Mapper2 and Reducer2 calculate the increase / decrease in the capacity per hall per year
//It also gives the minimum and maximum increase and decrease and the specific year when it was observed

public class CourseSchedule3 {

	public static class Mapper1	extends Mapper<Object, Text, Text, Text>{
		private final static Text capacity_val = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split(",");
		
			if(data.length > 11 || 
			   data[10].equals("0") || data[10].equals("1") ||
			   data[3].contains("2017"))
			{
				//Ignore				
			}
			else{				
				String[] hall = data[5].split("\\s+");
				String[] year = data[3].split("\\s+");

				//generate key
				String key_ec = hall[0] + "::" + year[1];				

				//set the value to # of current students enrolled and capacity
				int capacity = Integer.parseInt(data[10]);
				capacity_val.set("" + capacity);
				
				//both to be passed to reducer
				word.set(key_ec);
				context.write(word, capacity_val);				
			}
		}
	}


	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum_capacity = 0;
					
			for (Text val : values) {
				sum_capacity += Integer.parseInt(val.toString());				
			}
						
			result.set("" + sum_capacity);
			context.write(key, result);
		}
	}
	
	
	public static class Mapper2	extends Mapper<Object, Text, Text, Text>{
		
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split("\\t");
			
			String hall = data[0].split("::")[0];
			String year = data[0].split("::")[1];	
			
			String key1 = hall + "::" + year + " - " + hall + "::" + (Integer.parseInt(year)+1);
			String key2 = hall + "::" + (Integer.parseInt(year)-1) + " - " + hall + "::" + year;

			word.set(key1);
			context.write(word, new Text("-" + data[1]));
			word.set(key2);
			context.write(word, new Text(data[1]));		
		}
	}


	public static class Reducer2 extends Reducer<Text,Text,Text,Text> {
		
		static int max_increase = 0;
		static int max_decrease = 0;
		static String max_key = "";
		static String min_key = "";

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
									
			Iterator<Text> iterator = values.iterator();
			int capacity1 = Integer.parseInt(iterator.next().toString());
			if(!iterator.hasNext()) return;
			int capacity2 = Integer.parseInt(iterator.next().toString());
						
			capacity2 = capacity2 + capacity1;
			
			if(capacity2 > max_increase){
				max_increase = capacity2; 
				max_key = key.toString();
			}
			
			if(capacity2 < max_decrease) {
				max_decrease = capacity2; 
				min_key = key.toString();
			}
			
			
			context.write(key, new Text("" + capacity2));		
			context.write(new Text(), new Text("Max increase : " + max_increase + " for the year " + max_key));
			context.write(new Text(), new Text("Max decrease : " + max_decrease + " for the year " + min_key));			
		}
	}


	public static void main(String[] args) throws Exception {
		//String temp="Tepo";
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "map reduce 1");
		job1.setJarByClass(CourseSchedule3.class);
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		//System.exit(job1.waitForCompletion(true) ? 0 : 1);
		job1.waitForCompletion(true);// ? 0 : 1;
		
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "map reduce 2");
		job2.setJarByClass(CourseSchedule3.class);
		job2.setMapperClass(Mapper2.class);
		//job2.setCombinerClass(Reducer2.class);
		job2.setReducerClass(Reducer2.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
		
	}
}


