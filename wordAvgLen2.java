package comp9313.ass1;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;

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


public class WordAvgLen2 {
	public static class IntPair implements Writable{//implement customize Writable intPair<count,sum>
	  private int first,second;
	  public IntPair(){}
	  public IntPair (int first,int second){
		  set(first,second);
	  }
	  public void set(int left,int right){
		  first = left;
		  second = right;
	  }
	  public int getFirst(){
		  return first;
	  }
	  public int getSecond(){
		  return second;
	  }
	  public void write(DataOutput out) throws IOException{
		  out.writeInt(first);
		  out.writeInt(second);
	  }
	  public void readFields(DataInput in) throws IOException{
		  first = in.readInt();
		  second = in.readInt();
	  }
	}

        public static class TokenizerMapper extends Mapper<Object, Text, Text, IntPair> {
        	HashMap<String, IntPair> hash_table = new HashMap<>(); //<letter,IntPair> Using hashmap to inplement the in mapper combining                           
                private Text word = new Text();

                public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                        StringTokenizer itr = new StringTokenizer(value.toString(), " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
                        

                        while (itr.hasMoreTokens()) {

                                word.set(itr.nextToken().toLowerCase());
                                String line = word.toString();
                                String first_letter = line.substring(0,1);
                                if(first_letter.matches("[a-z]")){
                                int length = line.length();
                                int count = 1;
                                int sum = 0;

                                        if (hash_table.containsKey(first_letter)){
                                                                                  //If the first letter already in hashmap,update the hashmap
                                                count = hash_table.get(first_letter).getFirst()+1;
                                                sum = hash_table.get(first_letter).getSecond()+length;
                                                IntPair pair = new IntPair();
                                                pair.set(count,sum);
                                                hash_table.put(first_letter,pair);
                                                
                                        }else{
                                                                                   //If the first letter not in hashmap
                                        	
                                        	hash_table.put(first_letter,new IntPair(1,length));
                                        }


                                }
                        }
                }
                @Override
                protected void cleanup(Context context) throws IOException, InterruptedException {//In-mapper combining
                      	    
                      	         Text text_key = new Text();
                      	       
                      	         Set<String> keys = hash_table.keySet();
                      	         for (String s : keys) {
                      	        	text_key.set(s);
                      	        	
                      	        	int count = hash_table.get(s).getFirst();
                                    int sum = hash_table.get(s).getSecond();
                      	        	
                      	            context.write(text_key,new IntPair(count,sum));
                      	         }
                      	     }
                
        }

        public static class IntSumReducer extends Reducer<Text, IntPair, Text, DoubleWritable> {

                public void reduce(Text key, Iterable<IntPair> values, Context context)
                                throws IOException, InterruptedException {
                        double sum = 0;
                        int count = 0;
                        for (IntPair value : values){
                                
                                count+= value.getFirst();
                                sum += value.getSecond();

                }
                        context.write(key, new DoubleWritable(sum/count));

                }
        }

        public static void main(String[] args) throws Exception {
                Configuration conf = new Configuration();
                Job job = Job.getInstance(conf, "word count");
                job.setJarByClass(WordAvgLen2.class);
                job.setMapperClass(TokenizerMapper.class);
                //job.setCombinerClass(Combine.class);

                job.setReducerClass(IntSumReducer.class);
                job.setOutputKeyClass(Text.class);
                //job.setOutputValueClass(IntWritable.class);
                job.setOutputValueClass(IntPair.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]));
                System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
}
