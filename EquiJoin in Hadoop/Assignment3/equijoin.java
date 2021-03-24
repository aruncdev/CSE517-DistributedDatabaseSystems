import java.io.IOException;
import java.util.*;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class equijoin
{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		
		private Text join = new Text();
	    private Text data = new Text();
	    
	    @Override
	    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	    	
	    	String line = value.toString();
		    String[] val = line.split(",");
		    
	        String keyjoin = val[1];   
	        
	        join.set(keyjoin);
	        data.set(line);
	        
	        output.collect(join, data);
	    }
	}
	
	public  static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			List<String>  RRelation = new ArrayList<String>();
			List<String>  SRelation = new ArrayList<String>();
			
			Text joinVal = new Text();
			String tuple = "";
			String firstTable = null;
			
			while(values.hasNext()) {
				String val = values.next().toString();
				String[] valArr = val.split(",");
				
				if(firstTable == null) {
					firstTable = valArr[0];
				}
				
				if(firstTable.equalsIgnoreCase(valArr[0])) {
					RRelation.add(val);
				}
				else {
					SRelation.add(val);
				}
			}
			
			if(RRelation.size() == 0 || SRelation.size() == 0) {
				key.clear();
			}
			else {
				for(int i = 0; i < RRelation.size(); i++) {
					for(int j = 0; j < SRelation.size(); j++) {
						tuple= RRelation.get(i) + "," + SRelation.get(j);
						joinVal.set(tuple);
						output.collect(null, joinVal);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		JobConf config = new JobConf(equijoin.class);
		config.setJobName("equijoin");

		config.setOutputKeyClass(Text.class);
		config.setOutputValueClass(Text.class);
		   
		config.setMapperClass(Map.class);
		config.setReducerClass(Reduce.class);
	    
		FileInputFormat.setInputPaths(config,new Path(args[0]));	     
		FileOutputFormat.setOutputPath(config,new Path(args[1]));

		JobClient.runJob(config);
	}
}