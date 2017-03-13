import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.lang.*;
import java.util.Iterator;



/* 

Group Members : Ranganath Sundar(r26@buffalo.edu) - Person number = 50169271

   		Shalmalee Vishwasrao(shalmale@buffalo.edu) - Person number = 50169915



We have ignored the case "Before 8:00AM" because classes generally begin at/after 8:00 AM under normal circumstances 

and hence, the scheduling for the classes before 8:00 AM doesn't really help with our analysis.

*/



//Mapper1 and Reducer1 calculate the total number of students enrolled per hall per sem year AND total capacity per hall per sem year

//Mapper2 and Reducer2 calculate the utilization per hall per sem year



public class CourseSchedule5 {



	public static class Mapper1	extends Mapper<Object, Text, Text, Text>{

		private final static Text enrolled_capacity = new Text();

		//private final static Text capacity = new Text();

		private Text word = new Text();



		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



			//read record from csv file

			String line = value.toString();



			// use comma as separator

			String[] data = line.split(",");



			if(data[5].contains("Unknown") || data[5].contains("Arr Arr") || data[7].contains("Before 8:00AM")|| data[10].equals("0") || data[10].equals("1") || data.length > 11)

			{

				//Ignore				

			}

			else{

				//split location to get building name for key

				String[] hall = data[5].split("\\s+");



				//generate key

				String key1 = hall[0]+"_"+data[3];

				//String key_enrolled = hall[0]+"_"+data[3];

				//String key_capacity = hall[0]+"_"+data[3];



				//set the value to # of current students enrolled and capacity

				int enrolled = Integer.parseInt(data[9]);

				int capacity = Integer.parseInt(data[10]);

				enrolled_capacity.set(enrolled + "," + capacity);

				//capacity.set(Integer.parseInt(data[10]));



				//both to be passed to reducer

				word.set(key1);

				context.write(word, enrolled_capacity);

				//word.set(key_enrolled);

				//context.write(word, enrolled);

				//word.set(key_capacity);

				//context.write(word, capacity);				

			}

		}

	}





	public static class Reducer1 extends Reducer<Text,Text,Text,Text> {



		private Text result = new Text();



		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum_enrolled = 0;

			int sum_capacity = 0;

					

			for (Text val : values) {
					
				

				String[] ec = val.toString().split(",");

				int enrolled = Integer.parseInt(ec[0]);

				int capacity = Integer.parseInt(ec[1]);

				sum_enrolled += enrolled;

				sum_capacity += capacity;
				
				

			}

			

			String output = sum_enrolled + "," + sum_capacity;
			Text output1 = new Text();
		        output1.set(sum_enrolled+","+sum_capacity);



			//result.set(output1);

			context.write(key, output1);

		}

	}

	


	public static class Mapper2	extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable();

		private static Text enrolled_capacity1 = new Text();
		private Text word = new Text();



		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



			//read record from csv file

			String line = value.toString();

			// use comma as separator

			String[] data = line.split("\\t");
			String[] key0 = data[0].split("\\s+");
			String key_val = key0[1];
			System.out.println("The key is " + key_val);
			word.set(key_val);
			
			String[] val2 = data[1].split(",");
			String enr = val2[0];
			String cap = val2[1];
			
			int enrolled1 = Integer.parseInt(val2[0]);

			int capacity1 = Integer.parseInt(val2[1]);

			enrolled_capacity1.set(enrolled1 + "," + capacity1);
			System.out.println(enrolled1+","+capacity1);			
			context.write(word, enrolled_capacity1);

		}

	}





	public static class Reducer2 extends Reducer<Text,Text,Text,Text> {



		private Text result = new Text();
		//static String enr1 = "";
		//static String cap1 = "";
		static String out = "";
		static int sum_enr1 = 0;
		static int sum_cap1 = 0;



		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (Text val : values) {

				String[] val1 = val.toString().split(",");
				int enr1 = Integer.parseInt(val1[0]);
				int cap1 = Integer.parseInt(val1[1]);
				sum_enr1 += enr1;
				sum_cap1 += cap1;
			}

				
			
			String out = (((float)sum_enr1/(float)sum_cap1)*100)+"";

			result.set(out);

			context.write(key, result);

		}

	}
	
	public static class Mapper3	extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable();

		private static Text enrolled_capacity1 = new Text();
		private Text word = new Text();



		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



			//read record from csv file

			String line = value.toString();


			// use comma as separator

			String[] data = line.split("\\t");
			String[] key0 = data[0].split("\\s+");
			String[] key1 = key0[0].split("_");
			String key_val = key1[0];
			System.out.println("The key is " + key_val);
			word.set(key_val);
			
			String[] val2 = data[1].split(",");
			String enr = val2[0];
			String cap = val2[1];
			

			int enrolled1 = Integer.parseInt(val2[0]);

			int capacity1 = Integer.parseInt(val2[1]);

			enrolled_capacity1.set(enrolled1 + "," + capacity1);
			System.out.println(enrolled1+","+capacity1);			
			context.write(word, enrolled_capacity1);



			

		}

	}

	public static class Reducer3 extends Reducer<Text,Text,Text,Text> {



		private Text result = new Text();
		//static String enr1 = "";
		//static String cap1 = "";
		static String out = "";
		static int sum_enr1 = 0;
		static int sum_cap1 = 0;



		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum = 0;

			for (Text val : values) {

				String[] val1 = val.toString().split(",");
				int enr1 = Integer.parseInt(val1[0]);
				int cap1 = Integer.parseInt(val1[1]);
				sum_enr1 += enr1;
				sum_cap1 += cap1;
			}

				
			
			String out = (((float)sum_enr1/(float)sum_cap1)*100)+"";

			result.set(out);

			context.write(key, result);

		}

	}
	
	public static class Mapper4 extends Mapper<Object, Text, Text, Text>{

		//private final static IntWritable one = new IntWritable();

		private static Text enrolled_capacity1 = new Text();
		private Text word = new Text();



		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {



			//read record from csv file

			String line = value.toString();


			// use comma as separator

			String[] data = line.split("\\t");
			String[] key0 = data[0].split("\\s+");
			String[] key1 = key0[0].split("_");
			String key_val = key1[0]+"_"+key0[1];
			System.out.println("The key is " + key_val);
			word.set(key_val);
			
			String[] val2 = data[1].split(",");
			String enr = val2[0];
			String cap = val2[1];
			

			int enrolled1 = Integer.parseInt(val2[0]);

			int capacity1 = Integer.parseInt(val2[1]);

			enrolled_capacity1.set(enrolled1 + "," + capacity1);
			System.out.println(enrolled1+","+capacity1);			
			context.write(word, enrolled_capacity1);



			

		}

	}

	public static class Reducer4 extends Reducer<Text,Text,Text,Text> {



		private Text result = new Text();
		//static String enr1 = "";
		//static String cap1 = "";
		static String out = "";
		//int sum_enr1 = 0;
		//int sum_cap1 = 0;



		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			int sum = 0;
			int sum_enr1 = 0;
			int sum_cap1 = 0;

			for (Text val : values) {

				String[] val1 = val.toString().split(",");
				int enr1 = Integer.parseInt(val1[0]);
				int cap1 = Integer.parseInt(val1[1]);
				sum_enr1 += enr1;
				sum_cap1 += cap1;
			}
				
			
			String out = (((float)sum_enr1/(float)sum_cap1)*100)+"";

			result.set(out);

			context.write(key, result);

		}

	}
	
	
	public static class Mapper5	extends Mapper<Object, Text, Text, Text>{
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


	public static class Reducer5 extends Reducer<Text,Text,Text,Text> {
		
		static double max_increase = 0;
		static double max_decrease = 0;
		static String max_key = "";//new Text();
		static String min_key = "";//new Text();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			System.out.println("In reducer");
			Iterator<Text> iterator = values.iterator();
			double enrolled1 = Double.parseDouble(iterator.next().toString());
			if(!iterator.hasNext()) return;
			double enrolled2 = Double.parseDouble(iterator.next().toString());
			
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

		Configuration conf1 = new Configuration();

		Job job1 = Job.getInstance(conf1, "map reduce");

		job1.setJarByClass(CourseSchedule5.class);

		job1.setMapperClass(Mapper1.class);

		job1.setCombinerClass(Reducer1.class);

		job1.setReducerClass(Reducer1.class);

		job1.setOutputKeyClass(Text.class);

		job1.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));

		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.waitForCompletion(true);
		//Thread.sleep(1000);

		


		Configuration conf2 = new Configuration();

		Job job2 = Job.getInstance(conf2, "map reduce");

		job2.setJarByClass(CourseSchedule5.class);

		job2.setMapperClass(Mapper2.class);

		//job2.setCombinerClass(Reducer2.class);

		job2.setReducerClass(Reducer2.class);

		job2.setOutputKeyClass(Text.class);

		job2.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));

		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job2.waitForCompletion(true);
		
		
		Configuration conf3 = new Configuration();

		Job job3 = Job.getInstance(conf3, "map reduce");

		job3.setJarByClass(CourseSchedule5.class);

		job3.setMapperClass(Mapper3.class);

		//job2.setCombinerClass(Reducer2.class);

		job3.setReducerClass(Reducer3.class);

		job3.setOutputKeyClass(Text.class);

		job3.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job3, new Path(args[1]));

		FileOutputFormat.setOutputPath(job3, new Path(args[3]));

		job3.waitForCompletion(true);
		
		
		Configuration conf4 = new Configuration();

		Job job4 = Job.getInstance(conf4, "map reduce");

		job4.setJarByClass(CourseSchedule5.class);

		job4.setMapperClass(Mapper4.class);

		//job2.setCombinerClass(Reducer2.class);

		job4.setReducerClass(Reducer4.class);

		job4.setOutputKeyClass(Text.class);

		job4.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job4, new Path(args[1]));

		FileOutputFormat.setOutputPath(job4, new Path(args[4]));

		job4.waitForCompletion(true);
		
		
		Configuration conf5 = new Configuration();

		Job job5 = Job.getInstance(conf5, "map reduce");

		job5.setJarByClass(CourseSchedule5.class);

		job5.setMapperClass(Mapper5.class);

		//job2.setCombinerClass(Reducer2.class);

		job5.setReducerClass(Reducer5.class);

		job5.setOutputKeyClass(Text.class);

		job5.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job5, new Path(args[4]));

		FileOutputFormat.setOutputPath(job5, new Path(args[5]));

		System.exit(job5.waitForCompletion(true) ? 0 : 1);


	}

}

