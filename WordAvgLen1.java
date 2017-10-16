package comp9313.ass1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordAvgLen1 {

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		private Text word = new Text();		
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			while (itr.hasMoreTokens()) {
				
				
				word.set(itr.nextToken().toLowerCase());//Consider lower case
				String line = word.toString();
				String first_letter = line.substring(0,1);
				if(first_letter.matches("[a-z]")){ // Only consider starting from a to z
					
				int length = line.length();
				
				context.write(new Text(first_letter), new Text("1"+","+length+"")); // <key,value>=<text,"count,length"> Using text to transfer <count,length> pair
				}
			}
		}		
	}
    
    public static class Combine extends Reducer<Text,Text,Text,Text> {  
        
        // Reduce Method  
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            double sum = 0;  
            int count = 0;  
            for (Text value : values) {  
                String fields[] = value.toString().split(",");  //Split the <count,length> pair
                sum += Double.parseDouble(fields[1]);  
                count += Integer.parseInt(fields[0]);  
            }  
            context.write(key, new Text(count+","+sum));  
        }  
    } 
	
	public static class IntSumReducer extends Reducer<Text, Text, Text, DoubleWritable> {

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Text value : values){
				String fields[] = value.toString().split(",");
				sum += Double.parseDouble(fields[1]);
				count += Integer.parseInt(fields[0]);
				
		}
			context.write(key, new DoubleWritable(sum/count));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordAvgLen1.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(Combine.class); 
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
