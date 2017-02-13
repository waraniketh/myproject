package mostweeks;
//average of most weeksworked
import java.io.IOException;

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
public class Mostweeksincome {

	public static class Myreducer1 extends
	Reducer<Text, DoubleWritable, Text, DoubleWritable> {
protected void reduce(Text key, Iterable<DoubleWritable> values,
		Context context) throws IOException, InterruptedException {
	double sum=0; 
	int count=0;double finnalaverage=0;
	for(DoubleWritable retreive:values){
		double averagecal=retreive.get();
		sum=sum+averagecal;
		count++;
	}  
	finnalaverage=sum/count;
	context.write(new Text("finalaveragefor52weeks"),new DoubleWritable(finnalaverage));
}
}
	public static class Mymapper1 extends
	Mapper<LongWritable, Text, Text, DoubleWritable> {
protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String[] retreive=value.toString().split(",");
    if(retreive[9].equals("52")){
     context.write(new Text("caluculationofincome"),new DoubleWritable(Double.parseDouble(retreive[5])));	  
}}}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Mostweeksincome.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job,new Path("/dataforjava"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
