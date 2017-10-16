package comp9313.ass2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient.Conf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

//***********************************Transform Format  1st mapreduce***************************
public class SingleSourceSP {
        //Map functionality : value:0 0 1 5.0  =>  key(Node_from):0     value:1:5.0
        //Reduce functionality: key(Node_from):0        value:1:5.0  =>  key:0  value:0(distance_node_to_source) 1:2 2:5

        public static long last_count;
        public static enum COUNTER{   //Set a counter
                UPDATE_FLAG;
        };

        public static class FormatMapper extends Mapper<Object,Text,Text,Text>{
                String distance_node_to_source = "";

                @Override
                public void map(Object key,Text value,Context context)throws IOException, InterruptedException {

                        String fields[] = value.toString().split(" ");
                        String Node_from = fields[1];
                        String Node_to = fields[2];
                        String distance = fields[3];
                        Text out_key = new Text();
                        out_key.set(Node_from);
                        context.write(out_key, new Text(Node_to+":"+distance));

        }
        }

        public static class FormatReducer  extends Reducer<Text,Text,Text,Text>{
                String distance_node_to_source = "";

                public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
                        Configuration conf = context.getConfiguration();
                        String query = conf.get("queryID");
                        String result = "";

                        if (key.toString().equals(query)){
                                distance_node_to_source = "0";
                        }else{
                                distance_node_to_source = "INF";
                        }

                        for (Text value : values){
                                result+=" ";
                                result+= value.toString();

                        }
                        context.write(key,new Text(distance_node_to_source+result+" "+query));
                }

        }



        //***********************************Shortest Path 2nd mapreduce ****************************
        //input format:
        //input key:0  value:0  0(distance) 2:5 1:10 0(path)
        //input key:1  value:1  INF 3:1 2:2 0

        public static class PathMapper extends Mapper<Object,Text,Text,Text>{
                @Override
                public void map(Object key,Text value,Context context)throws IOException, InterruptedException {
                        String fields0[] = value.toString().split("\t");
                        String key_out = fields0[0];   //node
                        String others = fields0[1];    //0(distance) 2:5 1:10 0(path)

                        String fields[] = others.toString().split(" ");
                        String distance_node_to_source = fields[0];
                        int length = fields.length;
                        String path = fields[length-1];
                        for (int i=1;i<length-1;i++){   //Last field is path
                                String pair[] = fields[i].split(":");
                                String to_node = pair[0];       //adjacent node
                                String distance_from_two_nodes = pair[1];    //distance from this node to adjacent node

                                if (!(distance_from_two_nodes.equals("INF") || distance_node_to_source.equals("INF"))){

                                        double new_distance = Double.parseDouble(distance_from_two_nodes)+Double.parseDouble(distance_node_to_source);// find the smallest distance

                                        context.write(new Text(to_node),new Text(String.valueOf(new_distance)+" "+path+"->"+to_node));//(2,5 0->2)
                                //add path
                                }
                        }

                        context.write(new Text(key_out), new Text(others)); //add path
                }

        }

    // input key:0       value: 0(distance) 2:5 1:10 0->(path)
        public static class PathReducer  extends Reducer<Text,Text,Text,Text>{
                public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
                        Double shortest = 1e10;
                        String shortest_path = " ";
                        String graph_structure = "";

                        for (Text value : values){
                                String fields[] = value.toString().split(" ");
                                String distance_node_to_source = fields[0];

                                if (fields.length==2){   //If the text not include graph structure such as (2,5 0->2)

                                        if (Double.parseDouble(distance_node_to_source)<shortest){
                                                shortest = Double.parseDouble(distance_node_to_source);
                                                shortest_path = fields[1]; // renew path
                                                context.getCounter(COUNTER.UPDATE_FLAG).increment(1);
                                        }
                                }else{
                                                if (distance_node_to_source.equals("INF")){shortest_path = fields[fields.length-1];}
                                        if (!distance_node_to_source.equals("INF") && Double.parseDouble(distance_node_to_source)<shortest){
                                                shortest =Double.parseDouble(distance_node_to_source);
                                                shortest_path = fields[fields.length-1]; // renew path
                                                context.getCounter(COUNTER.UPDATE_FLAG).increment(1);
                                        }

                                        for (int i=1;i<fields.length-1;i++){// append graph structure
                                                graph_structure+=" ";
                                                graph_structure+=fields[i];
                                        }

                                }

                        }

                        context.write(key,new Text(shortest+graph_structure+" "+shortest_path));

                }

        }


        //***********************************Transform Format 3rd mapreduce***************************

        public static class FormatMapper2 extends Mapper<Object,Text,Text,Text>{
                @Override
                public void map(Object key,Text value,Context context)throws IOException, InterruptedException {
                        String fields0[] = value.toString().split("\t");
                        String node = fields0[0];
                        String others =fields0[1];
                        String fields[] = others.split(" ");
                        String distance = fields[0];
                        String path = fields[fields.length-1];
                        context.write(new Text(node), new Text(distance+"\t"+path));

                }
                }

        public static class FormatReducer2  extends Reducer<Text,Text,Text,Text>{
                public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
                        for (Text value : values){

                                context.write(key,value);
                        }
                        }
                }


    public static void main(String[] args) throws Exception {
        // *****************************Format Conversion ******************************

        String QUERY = args[2];
        Configuration conf = new Configuration();
        conf.set("queryID",QUERY);
                Job job = Job.getInstance(conf);



                job.setJarByClass(SingleSourceSP.class);
                job.setMapperClass(FormatMapper.class);
                job.setReducerClass(FormatReducer.class);
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job, new Path(args[0]));
                FileOutputFormat.setOutputPath(job, new Path(args[1]+"0"));
                job.waitForCompletion(true);


                //***************************** Iteration ******************************
                String output_first = args[1];
                String input_path = output_first;
                input_path += "0";
                int iteration = 0;
                boolean isdone = false;

                while (!isdone){
                        iteration++;

                        String output_path =output_first + String.valueOf(iteration);
                        Configuration conf2 = new Configuration();
                        Job job2 = Job.getInstance(conf2);

                        job2.setJarByClass(SingleSourceSP.class);
                        job2.setMapperClass(PathMapper.class);
                        job2.setReducerClass(PathReducer.class);
                        job2.setOutputKeyClass(Text.class);
                        job2.setOutputValueClass(Text.class);

                        FileInputFormat.addInputPath(job2, new Path(input_path));
                        FileOutputFormat.setOutputPath(job2, new Path(output_path));

                        job2.waitForCompletion(true);
                        Long count = job2.getCounters().findCounter(COUNTER.UPDATE_FLAG).getValue();
                        if (last_count == count){   //  If shortest path did not change, stop the iteration
                                isdone = true;
                        }else{
                                last_count = count;
                                input_path = output_path;
                        }

                }
                // *****************************Format Conversion ******************************

                input_path = output_first + String.valueOf(iteration);
                String output_path = args[1];

                Configuration conf3 = new Configuration();
                Job job3 = Job.getInstance(conf3);

                job3.setJarByClass(SingleSourceSP.class);
                job3.setMapperClass(FormatMapper2.class);
                job3.setReducerClass(FormatReducer2.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                FileInputFormat.addInputPath(job3, new Path(input_path));
                FileOutputFormat.setOutputPath(job3, new Path(output_path));

                job3.waitForCompletion(true);



    }
}