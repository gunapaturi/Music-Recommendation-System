package com.uncc.cloudProject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Vector;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.uncc.cloudProject.DataProcess.Map_DataProcess;
import com.uncc.cloudProject.DataProcess.Reduce_DataProcess;

/*
 * This class calculates Pearson Similarity Values
 */

public class Pearson_Calculation extends Configured implements Tool {

	public static void main( String[] args) throws  Exception 
	{

		long start_time = new Date().getTime();
		int res  = ToolRunner .run( new Similarity_Calculation(), args);
		long end_time = new Date().getTime();

		System.out.println("Time taken for Pearson Similarity ---->	" + (end_time - start_time) + "milliseconds");

		System .exit(res);
	}
	
	@Override
	public int run(String[] args) throws Exception 
	{
		Configuration conf_DataProcess = new Configuration();

		String intrmditeDirect = "intrmditeDirect";
		Path outputPath = new Path(args[1]);
		Path intermediatePath = new Path(intrmditeDirect);
		FileSystem hdfs = FileSystem.get(conf_DataProcess);

		try {
			if(hdfs.exists(outputPath)){
				hdfs.delete(outputPath, true);
			} 
			if(hdfs.exists(intermediatePath)){
				hdfs.delete(intermediatePath, true);
			} 
		} catch (IOException e) {
			e.printStackTrace();
		}

		Job job1  = Job .getInstance(conf_DataProcess, "DataProcess");
		job1.setJarByClass(Similarity_Calculation.class);

		Path Process_Output = new Path(intermediatePath, "ProcessOutput");		//intermediate file path is created

		FileInputFormat.addInputPaths(job1,  args[0]);					//input file path for job1
		FileOutputFormat.setOutputPath(job1, Process_Output);				//output file path for job1

		job1.setMapperClass( Map_DataProcess .class);
		job1.setReducerClass( Reduce_DataProcess .class);

		job1.setOutputKeyClass( Text .class);
		job1.setOutputValueClass( Text .class);

		int success1 =  job1.waitForCompletion( true)  ? 0 : 1;				//job2 executed after successfull completion of job1

		int success2 = 1;
		int success3 = 1;
		if(success1 == 0)
		{

			Configuration conf_PearsonSim = new Configuration();

			Job job2  = Job .getInstance(conf_PearsonSim, "Calculate_Pearson_Similarity");		
			job2.setJarByClass(Similarity_Calculation.class);

			Path PearsonSim_Output = new Path(intermediatePath, "PearsonSim_Output");

			FileInputFormat.addInputPath(job2,  Process_Output);			//input file path for job1
			FileOutputFormat.setOutputPath(job2, PearsonSim_Output);		//output file path for job2

			job2.setMapperClass( PearsonSimilarity_Map .class);
			job2.setReducerClass(PearsonSimilarity_Reduce .class);

			job2.setOutputKeyClass( Text .class);
			job2.setOutputValueClass( Text .class);

			success2 =  job2.waitForCompletion( true)  ? 0 : 1;			//job3 executed after sucessfull completion of job2


			if(success2 == 0)
			{
				Configuration conf3 = new Configuration();

				Job job3  = Job .getInstance(conf3, "PearsonSimilartyRecommendation");
				job3.setJarByClass(Similarity_Calculation.class);

				FileInputFormat.addInputPath(job3,  PearsonSim_Output);		//input file path for job3
				FileOutputFormat.setOutputPath(job3, outputPath);		//output file path for job3

				job3.setMapperClass( Map_Pearson_Recommendation .class);
				job3.setReducerClass( Reduce_Pearson_Recommendation .class);

				job3.setOutputKeyClass( Text .class);
				job3.setOutputValueClass( Text .class);

				success3 =  job3.waitForCompletion( true)  ? 0 : 1;
			}
		}      
		return success3;

	}
	
	public static class Map_Pearson_Recommendation extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			String[] parts = StringUtils.split(line);
			if(parts.length == 2){
				String songIDPair = parts[0];
				String rating = parts[1];

				String song1 = StringUtils.substringBefore(songIDPair, "##");
				String song2 = StringUtils.substringAfter(songIDPair, "##");

				context.write(new Text(song1), new Text(song2 + "=" + rating));
			}
		}
	}
	public static class Reduce_Pearson_Recommendation extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text songID,  Iterable<Text > songID_rating_list,  Context context)
				throws IOException,  InterruptedException {
	
			String recmd = "";

			for(Text item : songID_rating_list){
				recmd = recmd + item.toString() + ",";
			}

			recmd = recmd.substring(0, recmd.length()-1);
			context.write(songID, new Text(recmd));
		}
	}
	
	/*
	 * Mapper: PearsonSimilarity_Map
	 * Input: <uID sID=rating>
	 * output: <key, value> pairs -> <song1$$song2	rating1@@rating2>
     * Song Pairs are formed. Corresponding pairs of rating are sent to reducer.
	 */
	
	public static class PearsonSimilarity_Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			ArrayList<Integer> songList = new ArrayList<Integer>();
			ArrayList<Double> ratingList = new ArrayList<Double>();

			String line  = lineText.toString();

			String valuesList = StringUtils.substringAfter(line, "	");         
			String[] songRatingList = StringUtils.split(valuesList, ",");

			if(songRatingList.length >1){
				for(String songRating : songRatingList){

					String[] parts = StringUtils.split(songRating, "=");
					songList.add(Integer.parseInt(parts[0]));
					ratingList.add(Double.parseDouble(parts[1]));
				}

				int len = songList.size();

				for(int i=0; i<len-1; i++){
					for(int j=i+1; j<len; j++){

						String songPair = songList.get(i) + "$$" + songList.get(j);
						String ratingPair = ratingList.get(i) + "@@" + ratingList.get(j);

						context.write(new Text(songPair), new Text(ratingPair));   		 
					}
				}
			} 
		}
	}

	/*
	 * Reducer: PearsonSimilarity_Reduce
	 * Input: <song1$$song2	[list of rating1@@rating2] >
	 * Output: <key, value> pairs -> <song1$$song2 similarity_value>
	 * For every song pair, reducer calculates Pearson similarity. 
	 * Output is written only if the Pearson similarity value is greater than zero. Output form is song-pair and similarity score.
	 */
	
	public static class PearsonSimilarity_Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
		@Override 
		public void reduce( Text songPair,  Iterable<Text > ratingPairList,  Context context)
				throws IOException,  InterruptedException {

			Vector<Double> ratingVector1 = new Vector<Double>();
			Vector<Double> ratingVector2 = new Vector<Double>();

			double sum_1 = 0.0;
			double sum_2 = 0.0;

			for(Text val : ratingPairList){

				String ratings = val.toString();

				String[] rating = StringUtils.split(ratings, "@@");

				ratingVector1.addElement(Double.parseDouble(rating[0]));
				ratingVector2.addElement(Double.parseDouble(rating[1]));
			}

			if(ratingVector1.size() == ratingVector2.size() && ratingVector1.size() > 1){

				int len = ratingVector1.size();

				for(int i=0; i<len; i++){

					sum_1 = sum_1 + ratingVector1.get(i);
					sum_2 = sum_2 + ratingVector2.get(i);    			  
				}

				double average1 = sum_1 / len;	    	
				double average2 = sum_2 / len;			
				double denm1 = 0.0;
				double denm2 = 0.0;

				for(int i=0; i<len; i++){

					denm1 = denm1 + Math.pow(ratingVector1.get(i)-average1, 2);
					denm2 = denm2 + Math.pow(ratingVector2.get(i)-average2, 2);
				}

				double denm = Math.sqrt(denm1 * denm2);
				double pearsonCoeff = 0.0;

				for(int i=0; i<len; i++){

					double num = (ratingVector1.get(i) - average1) * (ratingVector2.get(i) - average2); 
					pearsonCoeff = pearsonCoeff + (num/denm);
				}

				if(Double.isNaN(pearsonCoeff)){
					pearsonCoeff = 0.0;
				} 
				if(pearsonCoeff > 0){
					context.write(songPair, new DoubleWritable(pearsonCoeff));
				}
			}
		}
	}
}
