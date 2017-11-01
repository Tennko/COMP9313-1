package comp9313.ass4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SetSimJoin {

	
public static class pairKey implements WritableComparable<pairKey>{ //Define custom WritableComparable used for sorting
		
		private String firstKey; 
		private String secondKey;
		public pairKey(){
		}

		public String getFirstKey(){
			return this.firstKey;
		}
		
		public String getSecondKey(){
			return this.secondKey;
		}
		
		public void setFirstKey(String first){
			this.firstKey = first;
		}
		
		public void setSecondKey(String second){
			this.secondKey = second;
		}
		
		public void readFields(DataInput in) throws IOException {
			this.firstKey=in.readUTF();
			this.secondKey=in.readUTF();
			
		}

		public void write(DataOutput out) throws IOException {
			out.writeUTF(firstKey);
			out.writeUTF(secondKey);
		}
		public int compareTo(pairKey pairKey) { //sort first key then sort second key
			if(this.firstKey.equals(pairKey.firstKey)){
				return Integer.compare(Integer.parseInt(this.secondKey), Integer.parseInt(pairKey.getSecondKey()));
			}
			else{
				Integer firstKey = Integer.parseInt(this.firstKey);
				Integer secondKey = Integer.parseInt(pairKey.getFirstKey());
				return firstKey.compareTo(secondKey);
			}
		}
	}
	
	
	
	//First mapreduce for finding out similar pairs
	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {	
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String fields[] = value.toString().split(" ");//example value = (0 1 4 5 6)
			String rid = fields[0]; //rid = 0
			String[] others = new String[fields.length-1]; //others = 1 4 5 6
			System.arraycopy(fields,1,others,0,fields.length-1);
			String others_out = "";
			for (int i =0;i<others.length;i++){
				if (i!=0){others_out+=" ";}
				others_out+=others[i];
			}
			
			Configuration conf = context.getConfiguration(); //get the threshold
			String t = conf.get("t"); 
			Double threshold = Double.valueOf(t);
			
			int p = (fields.length - 1 ) - (int) Math.ceil((fields.length-1)*threshold) +1;  //calculatge the prefix  p =|r| - l + 1 = |r| - ceil(|r|*t)+1
			for (int i =1;i<fields.length && i< p+1;i++){
				context.write(new Text(fields[i]),new Text(rid+ "," + others_out));//emit （key:1 value:0,1 4 5 6）,（key:4 value:0,1 4 5 6）..
			}
		}
  }

public static class Reduce extends Reducer<Text,Text,Text,Text> {  
	
        // Reduce Method  
    	public double getSim(String v1_content,String v2_content){ // function for obtain Jaccard similarity 
    		
    		Set  <String>set=new HashSet<String>();
    		String fields1[] = v1_content.split(" ");
    		String fields2[] = v2_content.split(" ");
    		double len1 = fields1.length;
    		double len2 = fields2.length;

    		for (String v:fields1){
    			set.add(v);
    		}
    		for (String v:fields2){
    			set.add(v);
    		}
    		double v1_or_v2 = set.size(); //Utilize a set to help 

    		double sim = (len1+len2-v1_or_v2)/v1_or_v2; 
    		return sim;
    	}

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
            Vector <String> vec = new Vector<String>();
            
            Configuration conf = context.getConfiguration();
			String t = conf.get("t");
			Double threshold = Double.valueOf(t);
			
            for (Text value : values){
            	vec.add(value.toString());
            }
            //Sort 
            Collections.sort(vec);
            
            int size=vec.size();
            String v1,v2;
            double sim;
            //Find out all the pairs
            for (int i =0;i<size-1;i++){
            	for (int j=i+1;j<size;j++){
            		v1 = vec.get(i);
            		v2 = vec.get(j);

            		String fields1[] = v1.split(",");
            		String v1_content = fields1[1];
            		String rid1 = fields1[0];
            		String fields2[] = v2.split(",");
            		String v2_content = fields2[1];
            		String rid2 = fields2[0];
            		
            		sim = getSim(v1_content,v2_content);
            		if (sim >= threshold){ // if similarity higher than threshold, output the result
            			if(Integer.parseInt(rid1)>Integer.parseInt(rid2)){   //make sure that rid1<rid2
            				context.write(key, new Text("("+rid2+","+rid1+")"+"\t"+sim));
            				}
            			else{
            			context.write(key, new Text("("+rid1+","+rid2+")"+"\t"+sim)); 
            			}
            		}
            	}
            }

        }  
    } 
	
//Second mapreduce for obtaining the format of final output

public static class Formatmapper extends Mapper<Object, Text, pairKey, Text> {	
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String fields[] = value.toString().split("\t"); // 1000\t(15,29)\t0.1111111111111111
		String pair = fields[1]; //pair = (15,29)
		String sim = fields[2];
		
		String keypair = pair.substring(1, pair.length()-1);//keypair = 15,29
		String pairFields[] = keypair.split(",");
		pairKey newkeypair = new pairKey();
		newkeypair.setFirstKey(pairFields[0]);
		newkeypair.setSecondKey(pairFields[1]);
		
		//System.out.println(pairFields[0]+ " "+pairFields[1]);
		
		context.write(newkeypair,new Text(sim)); //automatically sorting of pairkey here in mapper
	}
	
}

public static class Formatreducer extends Reducer<pairKey, Text, Text, Text> {
	public void reduce(pairKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		String outkey = "(";
		outkey += key.getFirstKey() +"," + key.getSecondKey() + ")";
		
		for(Text value:values){
			context.write(new Text(outkey),value);
			break;
		}
	}
}


  public static void main(String[] args) throws Exception {
	String output = args[1];
    Configuration conf = new Configuration();
    conf.set("t",(args[2]));
    
    int number_of_reducers = Integer.parseInt(args[3]);
    Job job = Job.getInstance(conf, "SetSimJoin");

    job.setJarByClass(SetSimJoin.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    
    job.setNumReduceTasks(number_of_reducers);
    
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(output+"_"));
    job.waitForCompletion(true);
    
    //**************
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "SetSimJoin");
    job2.setJarByClass(SetSimJoin.class);
    job2.setMapperClass(Formatmapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(Formatreducer.class);
    
    job2.setMapOutputKeyClass(pairKey.class);
    job2.setMapOutputValueClass(Text.class);
    
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    
    job2.setNumReduceTasks(1);
    
    FileInputFormat.addInputPath(job2, new Path(output+"_"));
    FileOutputFormat.setOutputPath(job2, new Path(output));
    job2.waitForCompletion(true);
  }
}
