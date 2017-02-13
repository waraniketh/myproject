package firstquery;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Finalaverage {


	public static class Mymapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] values=value.toString().split("\t");
			context.write(new Text("difference"),new Text(values[1]));
		}}
	public static class Myreducer1 extends
	Reducer<Text, Text, Text, DoubleWritable> {
protected void reduce(Text key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
	Iterator<Text> itr=values.iterator();int i=0;double[] difference=new double[2];
	while(itr.hasNext()){
		difference[i]=Double.parseDouble(itr.next().toString());
		i++;
	}
	double finnalaverage=((difference[1]-difference[0])*100)/difference[0];
	context.write(new Text("finalaverage"),new DoubleWritable(finnalaverage));
}}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Finalaverage.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job, new Path("/outputforaverage"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
