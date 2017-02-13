package mostweeks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Personwithmostworkingweeks {
	public static class Mymapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] retreive = value.toString().split(",");
			context.write(new Text("weeksworked"), new Text(retreive[9] + ","
					+ retreive[5]));
		}
	}

	public static class Myreducer1 extends
			Reducer<Text, Text, Text, DoubleWritable> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			TreeMap<Double, Text> weekworkedmap = new TreeMap<Double, Text>();
			String weekvalue = "";
			Iterator<Text> itr = values.iterator();
			ArrayList income = new ArrayList();
			int i = 0;
			while (itr.hasNext()) {
				String[] str = itr.next().toString().split(",");
				weekvalue = str[0];
				income.add(i++, str[1] + "," + str[0]);
				weekworkedmap.put(new Double(Double.parseDouble(weekvalue)),
						key);
			}
			double check = weekworkedmap.descendingMap().firstKey();
			Iterator itr1 = income.iterator();
			double finnalmax = 0.0;
			while (itr1.hasNext()) {
				String str2 = (String) itr1.next();
				String[] str3 = str2.split(",");
				if (check == Double.parseDouble(str3[1])) {
					if (finnalmax < Double.parseDouble(str3[0])) {
						finnalmax = Double.parseDouble(str3[0]);

					}
				}
			}context.write(new Text("maxincomeofmaxweeksworked"),new DoubleWritable(finnalmax));
			context.write(new Text("maxincomeofmaxweeksworked"),new DoubleWritable(check));
		}
	}

	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Personwithmostworkingweeks.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job, new Path("/dataforjava"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
