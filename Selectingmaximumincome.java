package mostweeks;
//top income 
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

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

public class Selectingmaximumincome {
	public static class Mymapper1 extends
	Mapper<LongWritable, Text, Text, DoubleWritable> {
protected void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
	String[] retreive=value.toString().split(",");
context.write(new Text("maximumincome"),new DoubleWritable(Double.parseDouble(retreive[5])));
}}
	public static class Myreducer1 extends
	Reducer<Text, DoubleWritable, Text, DoubleWritable> {
protected void reduce(Text key, Iterable<DoubleWritable> values,
		Context context) throws IOException, InterruptedException {
	TreeMap<Double,String> max=new TreeMap<Double,String>();
	Double average=0.0;
	for(DoubleWritable retreive:values){
		max.put((Double)retreive.get(),"maximumincome");
	}
	NavigableMap<Double, String> nav=max.descendingMap();
	Set<Double> set=nav.keySet();
	Iterator<Double> itr=set.iterator();
	int check=0;
	Double sum=0.0;
	while(itr.hasNext()){
	if(check>=5){
    break; 		
	}	
	else
	{
		sum=sum+itr.next();
	}	
	check++;	
	}
	average=sum/5;
	context.write(new Text("averageoftop5"),new DoubleWritable(average));
	context.write(new Text("maximumincome"),new DoubleWritable(nav.firstKey()));
}}
	public static void main(String[] args) throws IllegalArgumentException, IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Selectingmaximumincome.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job,new Path("/dataforjava"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}
}


