package firstquery;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Checkingagegroup {

	public static class Mymapper1 extends
			Mapper<LongWritable, Text, Text,Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (columns[1].equals("Doctorate degree(PhD EdD)")) {
				context.write(new Text("highesteducationlevel"), new Text(columns[0]+","+"1"));
			}
			if (columns[1].equals("Masters degree(MA MS MEng MEd MSW MBA)")) {
				context.write(new Text("highesteducationlevel"), new Text(columns[0]+","+"1"));
			}
		}
	}

	public static class Myreducer1 extends
			Reducer<Text, Text, Text, IntWritable> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count=0;
			for(Text valu:values){
        	String[] retreive=valu.toString().split(",");
            int count1=Integer.parseInt(retreive[1]);
			count=count+count1;
			}
          context.write(key,new IntWritable(count));
		}
	}

	public static class Mypartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text age, Text arg1, int arg2) {
			int returnpartition = 0;
			String[] ageverify=arg1.toString().split(",");
			int ageverify1=Integer.parseInt(ageverify[0]);
			if (ageverify1 > 14 && ageverify1 < 20) {
				returnpartition = 0;
			} else if (ageverify1 >= 20 && ageverify1 < 40) {
				returnpartition = 1;
			} else if (ageverify1 >= 40 && ageverify1 < 60) {
				returnpartition = 2;
			} else if (ageverify1 >= 60 && ageverify1 < 80) {
				returnpartition = 3;
			}
			return returnpartition;
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Checkingagegroup.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setNumReduceTasks(4);
		job.setReducerClass(Myreducer1.class);
		job.setPartitionerClass(Mypartitioner.class);
		FileInputFormat.addInputPath(job, new Path("/dataforjava"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
