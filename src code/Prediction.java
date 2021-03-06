package com.uncc.cloudProject;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Enumeration;
import java.util.Vector;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.io.FileNotFoundException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

@SuppressWarnings("unused")
public class Prediction extends Configured implements Tool {
      public static void main( String[] args) throws  Exception {
      int result  = ToolRunner .run( new Prediction(), args);
      System .exit(result);
   	}
      public int run( String[] args) throws  Exception {	   
	   	Configuration predictConf = new Configuration();	   
		FileSystem hdfs = FileSystem.get(predictConf);
	   	String intPath = "intPath";
	   	Path outputPath = new Path(args[3]);
	   	Path intermediatePath = new Path(intPath);
	   	try {
		   	if(hdfs.exists(outputPath)){
			   hdfs.delete(outputPath, true);
		   } if(hdfs.exists(intermediatePath)){
			   hdfs.delete(intermediatePath, true);
		   }				
		} catch (IOException e) {
				e.printStackTrace();
		}	   
	   	predictConf.set("SimilarityPath", args[1]);				//Similarity Data is set
	   	predictConf.set("TestDataPath", args[2]);				//Test Data is set
      	Job job = Job .getInstance(predictConf, "Prediction");
      	job.setJarByClass(Prediction.class);
      	Path intermediate1 = new Path(intermediatePath, "intermediate1");		//An intermediate path is created 
      	FileInputFormat.addInputPaths(job,  args[0]);							//Input File Path is set for job1
      	FileOutputFormat.setOutputPath(job, intermediate1);						//Intermediate path is set for the output for first job
      	job.setMapperClass( Prediction_Map .class);
      	job.setMapOutputKeyClass(Text.class);
      	job.setMapOutputValueClass(Text.class);      	
      	job.setReducerClass( Prediction_Reduce .class);
		job.setNumReduceTasks(1);  
      	int success =  job.waitForCompletion( true)  ? 0 : 1;					//if job is successfully completed, job2 is executed
      	
		if(success == 0){    	  
    	  	Configuration conf_Accurancy = new Configuration();    	  
         	Job job2  = Job .getInstance(conf_Accurancy, "Accuracy");
         	job2.setJarByClass(Prediction.class);         
         	Path intermediateOutput = new Path(intermediate1, "part-r-00000");	//An intermediate output path is set (from job1)
         	FileInputFormat.addInputPath(job2,  intermediateOutput);			//Input file path is set for job2
         	FileOutputFormat.setOutputPath(job2, outputPath);         			//Output file path is set for job2
         	job2.setMapperClass( Accuracy_Mapper .class);
         	job2.setMapOutputKeyClass(Text.class);
         	job2.setMapOutputValueClass(IntWritable.class);
      		job2.setReducerClass( Accuracy_Reducer .class);
		int success2 =  job2.waitForCompletion( true)  ? 0 : 1;
	}
	return 0;
	}

      /*
       * Mapper: Prediction_Map
       * Input: <UID SID Rating>
       * Output: <UID(key) (SID, Rating)Value)
       */
      
   public static class Prediction_Map extends Mapper<LongWritable ,  Text ,  Text ,Text   > {	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {    	    
         String inputline  = lineText.toString();
         String[] parts = StringUtils.split(inputline);
			if(parts.length == 3){
				String UID = parts[0];						//UserID is saved		
				String SID = parts[1];						//SongID is saved
				String rating = parts[2];					//Rating is saved
				context.write(new Text(UID), new Text(SID + "," + rating));
         }
      }
   }


   /*
    * Reducer: Prediction_Reduce
    * Input: <UID SID,Rating>
    * Output: <UID SID ActualRating PredictRating
    */
   
	public static class Prediction_Reduce extends Reducer<Text, Text, Text, Text> {
		private Map<Integer, Map<Integer, Float>> SimilarityMap = new HashMap<Integer, Map<Integer, Float>>();
		private Map<String, List<String>> inputFileList = new HashMap<String, List<String>>();
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			Map<Integer,Integer> userSongList = new HashMap<Integer,Integer>();			
			for (Text value : values){
				int i = Integer.parseInt(value.toString().split(",")[0].trim());
				int r = Integer.parseInt(value.toString().split(",")[1].trim());
				userSongList.put(i,r);
			}
			for (String val : inputFileList.get(key.toString())) {			
				String itemId = val.split(",")[0];
				String rating = val.split(",")[1];
				int pR = Math.round(predictRating(Integer.parseInt(itemId.trim()), userSongList));
				Text k = new Text(key.toString() + "\t" + itemId);
				Text v = new Text(rating + "\t" + Integer.toString(pR));
				context.write(k, v); 				
			}
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 * This method will be executed at the start of the task
		 */
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration confR = context.getConfiguration();
			String simFileName = confR.get("SimilarityPath");
			String testFilePath = confR.get("TestDataPath");
			loadingInputdata(testFilePath);
			loadingSimdata(simFileName);
		}
		/*
		 * PredictRating will be calculated using this method
		 */
		private float predictRating(int itemId, Map<Integer,Integer> userlist)
		{
			float simRateSum = 0, similaritySum = 0;
			for (Integer item : userlist.keySet()) {
				float similarity=0;
				int sID = item;
				int lID = itemId;
				int rating = userlist.get(item);			
				if( itemId < sID){
					int t = lID;
					lID = sID;
					sID = t;
				}
				if (SimilarityMap.containsKey(sID) && SimilarityMap.get(sID).containsKey(lID)) {
					similarity = SimilarityMap.get(sID).get(lID);
				}
				else {
					similarity = 0; 
				}			
				simRateSum += similarity * rating;
				similaritySum += similarity;
			}
			float pR = 0;
			if(similaritySum > 0)
				pR = simRateSum/similaritySum;
			return pR;
		}
		/*
		 * This method is used to load Similarity program output
		 */	
		private void loadingSimdata(String simFileName) throws FileNotFoundException, IOException {
			Path pt=new Path(simFileName);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String inputline = null;
			while ((inputline = br.readLine()) != null) {
				String[] tokens = inputline.split("\t");
				int item1 = Integer.parseInt(tokens[0]);
				String[] itemSimList = tokens[1].split(",");
				int i = itemSimList.length;
				Map<Integer, Float> tmp = new HashMap<Integer, Float>();
				for( int m =0; m<i;m++)
				{
					String item_2 = itemSimList[m].split("=")[0];
					Float sim = Float.parseFloat(itemSimList[m].split("=")[1]);
					int item2 = Integer.parseInt(item_2);
					tmp.put(item2, sim);	
				}
				SimilarityMap.put(item1, tmp);
			}
			br.close();
		}
		/*
		 * This method is used to load input data
		 */
		private void loadingInputdata(String testFilePath) throws FileNotFoundException, IOException {
			Path pt=new Path(testFilePath);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String inputline = null;	
			while ((inputline = br.readLine()) != null) {
				String[] tokens = inputline.split("\t");
				String uID = tokens[0];
				String iID = tokens[1];
				String rating = tokens[2];
				String song_rating = iID + "," + rating;		
				if (inputFileList.containsKey(uID)) {
					inputFileList.get(uID).add(song_rating);
				} 
				else {
					List<String> tmp = new ArrayList<String>();
					tmp.add(song_rating);
					inputFileList.put(uID, tmp);
				}
			}
			br.close();
		}
	}
//Mapper class used to calculate Accuracy 
public static class Accuracy_Mapper extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > 
{	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {    	    
         String inputline  = lineText.toString();
         String[] parts = StringUtils.split(inputline);
         if(parts.length == 4){
        	 String key = parts[0]+parts[1];
        	 int aR = Integer.parseInt(parts[2].trim());
        	 int pR = Integer.parseInt(parts[3].trim());        	 
        	 context.write(new Text("ErrorCount"), new IntWritable(Math.abs(aR - pR)));
         }
      }
 }

//Reducer class used to calculate Accuracy
public static class Accuracy_Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {			
			int result = 0;
			double sum = 0;
			for (IntWritable val : values) {
				result++;
				sum += val.get(); 
			}
			double accuracy = sum/result;
				context.write(new Text("Accuracy: "), new DoubleWritable(accuracy)); 
			}
		}
}
