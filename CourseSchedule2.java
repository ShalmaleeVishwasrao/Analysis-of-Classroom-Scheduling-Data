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

We have ignored the case "Before 8:00AM" because classes generally begin at/after 8:00 AM under normal circumstances 
and hence, the scheduling for the classes before 8:00 AM doesn't really help with our analysis.
*/

//Mapper1 and Reducer1 calculate the total number of students enrolled per hall per sem year AND total capacity per hall per sem year
//Mapper2 and Reducer2 calculate the utilization per hall per sem year

public class CourseSchedule2 {

	public static class Mapper1	extends Mapper<Object, Text, Text, Text>{
		private final static Text enrolled_val = new Text();
		//private final static Text capacity_val = new Text();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split(",");

			if(data.length > 11 || 
			   data[5].contains("Unknown") || 
			   data[5].contains("Arr Arr") || 
			   data[7].contains("Before 8:00AM")|| 
			   data[10].equals("0") || data[10].equals("1"))
			{
				//Ignore				
			}
			else{				
				String[] year = data[3].split("\\s+");

				//generate key
				String dept = data[4];	
				String yr = year[1];
				String key_1 =dept+"_"+yr; 

				//set the value to # of current students enrolled and capacity
				int enrolled = Integer.parseInt(data[9]);
				//int capacity = Integer.parseInt(data[10]);
				enrolled_val.set("" + enrolled);

				//enrolled_capacity.set("" + (enrolled / capacity)*100);
				
				//both to be passed to reducer
				word.set(key_1);
				context.write(word, enrolled_val);				
			}
		}
	}


	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {

		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum_enrolled = 0;
			//int sum_capacity = 0;
			//double total_util = 0.0;
			//int count = 0;
					
			for (Text val : values) {
				//String[] ec = val.toString().split(",");
				//int enrolled = Integer.parseInt(ec[0]);
				//int capacity = Integer.parseInt(ec[1]);
				//sum_enrolled += enrolled;
				//sum_capacity += capacity;
				//total_util += Double.parseDouble(val.toString());
				sum_enrolled += Integer.parseInt(val.toString());
				//count++;
			}
			
			//String output = "" + (sum_enrolled / sum_capacity) * 100;
			//result.set(output);
			//result.set("" + total_util/count);
			result.set("" + sum_enrolled);
			context.write(key, result);
		}
	}
	
	
	public static class Mapper2	extends Mapper<Object, Text, Text, Text>{
		//private final static IntWritable one = new IntWritable();
		private Text word = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			//read record from csv file
			String line = value.toString();

			// use comma as separator
			String[] data = line.split("\\t");
			String[] year = data[0].split("_");
			
			String key1 = year[0]+"_"+year[1] + " - " + year[0]+"_"+(Integer.parseInt(year[1])+1);
			String key2 = year[0]+"_"+(Integer.parseInt(year[1])-1) + " - " + year[0]+"_"+year[1];
			
			System.out.println(key1 + data[1]);
			System.out.println(key2 + data[1]);
			word.set(key1);
			context.write(word, new Text("-"+data[1]));
			word.set(key2);
			context.write(word, new Text(data[1]));
			System.out.println("in mapper 2");
			
		}
	}


	public static class Reducer2 extends Reducer<Text,Text,Text,Text> {
		
		static int max_increase = 0;
		static int max_decrease = 0;
		static String max_key = "";//new Text();
		static String min_key = "";//new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("In reducer");
			Iterator<Text> iterator = values.iterator();
			int enrolled1 = Integer.parseInt(iterator.next().toString());
			if(!iterator.hasNext()) return;
			int enrolled2 = Integer.parseInt(iterator.next().toString());
			
			enrolled2 = enrolled1 + enrolled2;
			if(enrolled2 > max_increase){
				max_increase = enrolled2; 
				max_key = key.toString();
			}
			
			//calculate the maximum decrease so far
			if(enrolled2 < max_decrease) {
				max_decrease = enrolled2; 
				min_key = key.toString();
			}

			System.out.println("Value to be written is : " + enrolled2);
			context.write(key, new Text("" + enrolled2));	
			context.write(new Text(), new Text("Max increase : " + max_increase + " for  " + max_key));
			context.write(new Text(), new Text("Max decrease : " + max_decrease + " for  " + min_key));	
		}
	}


	public static void main(String[] args) throws Exception {
		//String temp="Tepo";
		Configuration conf1 = new Configuration();
		Job job1 = Job.getInstance(conf1, "map reduce 1");
		job1.setJarByClass(CourseSchedule2.class);
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
		job2.setJarByClass(CourseSchedule2.class);
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









