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

//Mapper1 and Reducer1 calculate the total number of students enrolled per year
//Mapper2 and Reducer2 calculate the increase / decrease in the number of enrollments per year
//It also gives the minimum and maximum increase and decrease and the specific year when it was observed

public class CourseSchedule1 {

	public static class Mapper1	extends Mapper<Object, Text, Text, Text>{
		private final static Text enrolled_val = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split(",");
			
			if(data.length > 11 || data[10].equals("0") || data[10].equals("1") || data[3].contains("2017"))
			{
				//Ignore				
			}
			else{				
				String[] year = data[3].split("\\s+");

				//generate key
				String key_ec = year[1];				

				//set the value to # of current students enrolled and capacity
				int enrolled = Integer.parseInt(data[9]);
				enrolled_val.set("" + enrolled);
				
				//pass to reducer
				word.set(key_ec);
				context.write(word, enrolled_val);				
			}
		}
	}


	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum_enrolled = 0;
					
			for (Text val : values) {				
				sum_enrolled += Integer.parseInt(val.toString());
			}
						
			result.set("" + sum_enrolled);
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
			
			String key1 = data[0] + " - " + (Integer.parseInt(data[0])+1);
			String key2 = (Integer.parseInt(data[0])-1) + " - " + data[0];

			word.set(key1);
			context.write(word, new Text("-" + data[1]));
			word.set(key2);
			context.write(word, new Text(data[1]));			
		}
	}


	public static class Reducer2 extends Reducer<Text,Text,Text,Text> {
		
		static int max_increase = 0;
		static int max_decrease = 0;
		static int max_key = 0;//new Text();
		static int min_key = 0;//new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					
			Iterator<Text> iterator = values.iterator();
			
			int enrolled1 = Integer.parseInt(iterator.next().toString());
			if(!iterator.hasNext()) return;
			int enrolled2 = Integer.parseInt(iterator.next().toString());
				
			//calculate the increase or decrease in the 			
			int enrolled = 0;
			enrolled = enrolled2 + enrolled1;
			
			//compute the maximum increase so far
			if(enrolled > max_increase){
				max_increase = enrolled; 
				max_key = Integer.parseInt(key.toString().split(" - ")[0]);
			}
			
			//calculate the maximum decrease so far
			if(enrolled < max_decrease) {
				max_decrease = enrolled; 
				min_key = Integer.parseInt(key.toString().split(" - ")[0]);
			}
			
			//System.out.println("Value to be written is : " + enrolled2);
			context.write(key, new Text("" + enrolled));		
			context.write(new Text(), new Text("Max increase : " + max_increase + " for the year " + max_key));
			context.write(new Text(), new Text("Max decrease : " + max_decrease + " for the year " + min_key));			
		}
	}


	public static void main(String[] args) throws Exception {
		//String temp="Tepo";
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "map reduce 1");
		job1.setJarByClass(CourseSchedule1.class);
		job1.setMapperClass(Mapper1.class);
		job1.setCombinerClass(Reducer1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		job1.waitForCompletion(true);
		
		
		Configuration conf2 = new Configuration();
		Job job2 = Job.getInstance(conf2, "map reduce 2");
		job2.setJarByClass(CourseSchedule1.class);
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









