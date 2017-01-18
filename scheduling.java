//Map Reduce Code for generating key value pairs - (Parallel Data Processing Using Hadoop). This code also performs data clean-up/refactoring operation on the input data.

/*
	Author : Vishwaksen Mane
*/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// Scheduling Class refactors the input data and generates the corresponding key value pairs.

public class scheduling {
public static class TokenizerMapper
extends Mapper<Object, Text, Text, IntWritable>{
private final static IntWritable val1 = new IntWritable();
private Text word = new Text();
public void map(Object key, Text value, Context context
) throws IOException, InterruptedException {	
StringTokenizer itr = new StringTokenizer(value.toString(),",");
int i=0,flag=0,flg,count;
String arr = "Arr ";
boolean b;
String val=null;
flg=0;
b = false;
count=0;
String semester=null,building=null;
while(itr.hasMoreTokens()){
	flag=0;
	if(i==1){
	 semester = itr.nextToken();
	 flag=1;
	}
	if(i==2) {
	 building = itr.nextToken();
	 if(building.contains("Unknown") || building.contains(arr))
	 {
		 flg=0;
		 count=-1;
	 }
	 else {
		 flg=1;
		 count = count + 1;
	 }
	 flag=1;
	}
	 if(i==7) {
	 val = itr.nextToken();
	 b = val.matches("\\d+");
	 if(b==true) {
		 flg=1;
		 count = count + 1;
	 }
	 flag=1;
	}
	else if(flag==0)
		itr.nextToken();
	i++;
}
if(flg==1 && count == 2)
{
String[] key1 = building.split(" ");
String res = key1[0] + "_" + semester;
IntWritable val1  = new IntWritable(Integer.parseInt(val));
word.set(res);
context.write(word, val1);
}
}
}


//REDUCER FUNCTION

public static class IntSumReducer
extends Reducer<Text,IntWritable,Text,IntWritable> {
private IntWritable result = new IntWritable();
public void reduce(Text key, Iterable<IntWritable> values,
Context context
) throws IOException, InterruptedException {
int sum = 0;
for (IntWritable val : values) {
sum += val.get();
}
result.set(sum);
context.write(key, result);
}
}


//MAIN FUNCTION

public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "class room scheduling");
job.setJarByClass(scheduling.class);
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);
job.setReducerClass(IntSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}