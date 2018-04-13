package com.uncc.cloudProject;
import java.io.*;
import java.util.*;
import java.io.BufferedReader;
import java.util.Set;
import java.util.Map;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map.Entry;
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class Recommendation extends Configured implements Tool {
      public static void main( String[] args) throws  Exception {
      		int result  = ToolRunner .run( new Recommendation(), args);
      		System .exit(result);
   	}
   	@SuppressWarnings("unused")
	public int run( String[] args) throws  Exception {	  
	   	Configuration conf_recommend = new Configuration();
	  	Path output = new Path(args[2]); //output variable has the command line argument which is the output path
 		conf_recommend.set("similaritypath", args[1]);//similarityPath variable has the similarity File path
		FileSystem hdfs = FileSystem.get(conf_recommend);	   
	   	try {
		   	if(hdfs.exists(output)){
			   	hdfs.delete(output, true);// If there is any output file path before, it will be deleted
		   	} 
		}
		catch (IOException e) {
				e.printStackTrace();
		}	   
      	Job job = Job .getInstance(conf_recommend, "Recommendation");//First job instance is created
      	job.setJarByClass(Recommendation.class);      
      	FileInputFormat.addInputPaths(job,  args[0]);//input file path is set by taking the first command line argument
      	FileOutputFormat.setOutputPath(job, output);      
      	job.setMapperClass( Map_Recommendation .class);// Mapper class is set
      	job.setReducerClass( Reduce_Recommendation .class);//Reducer class is set     
      	job.setOutputKeyClass( Text .class);
      	job.setOutputValueClass( Text .class);
      	int success =  job.waitForCompletion( true)  ? 0 : 1;
	return 0;
	}
   	/* Mapper Class: Map_Recommendation
   	 * Input for Mapper class is of the form <userID		songId		rating> 
   	 * Output for Mapper class is of the form <userID(key), (songID,rating)(value))
   	 * 
   	 */
   public static class Map_Recommendation extends Mapper<LongWritable ,  Text ,  Text ,  Text > {	   
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {    	    
         String inputline  = lineText.toString();
         String[] parts = StringUtils.split(inputline);
         if(parts.length == 3){
        	 String uID = parts[0];					//UserID is stored
        	 String sID = parts[1];					//SongID is stored
        	 String rating = parts[2]; 				//Rating is stored       	 
        	 context.write(new Text(uID), new Text(sID + "," + rating)); 		//Key and Value pair is written in the output file
         }
      }
   }
	/* Reduce Class: Reduce_Recommendation
  	 * Input for Reducer class is of the form <userID, (songID,rating)> 
  	 * Output for Reducer class is of the form <>
  	 * 
  	 */
    public static class Reduce_Recommendation extends Reducer<Text, Text, Text, Text> {
		private Map<Integer, String> SimilarityMap = new HashMap<Integer, String>();//A HashMap is created, to store similarity list values
		@SuppressWarnings("unused")
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {			
			Map<Integer,Integer> user_list = new HashMap<Integer,Integer>();			//Declaring a HashMap to store list of songs which user has listened from the data set
			Map<Integer,Float> recom_hmap = new HashMap<Integer,Float>();
			String sIDRating = "";													//The songs which are liked by the user are stored in this variable						
			for (Text value : values){
				String sid=value.toString().split(",")[0].trim();					//<songId,rating> songId is stored on SID		
				if(sIDRating.equals(""))
					sIDRating = sid;
				else					
					sIDRating = sIDRating + "," + sid;
				int i = Integer.parseInt(sid);
				int r = Integer.parseInt(value.toString().split(",")[1].trim());	//storing the rating value
				user_list.put(i,r);
			}
			
			//To the list of songs liked by user, top recommended songs are added
			
			for (int itemId : user_list.keySet()) {				
				int user_song = itemId;
				boolean flag = false;
				String sim_list = "";
				if( SimilarityMap.containsKey(user_song)){
					sim_list = SimilarityMap.get(user_song);						
					flag = true;
				}
				if(flag){
					String[] a = sim_list.split(","); 				
    					Arrays.sort(a, new Comparator<String>() { 				//sort operation is performed on similarity list of songs
     						 public int compare(String string1, String string2) {
          					 String s1 = string1.substring(string1.lastIndexOf("=")+1);
         					 String s2 = string2.substring(string2.lastIndexOf("=")+1);
          					return Double.valueOf(s2).compareTo(Double.valueOf(s1));}});					
					for(int i=0; i< a.length ; i++)
					{
						int song = Integer.parseInt(a[i].split("=")[0]);
						float sim = Float.parseFloat(a[i].split("=")[1]);
						float p = predictRating(song, user_list);					//predicting the rating for similar song is calculated
						recom_hmap.put(song,p);
						if( SimilarityMap.containsKey(song))
						{	
							if( p >= recom_hmap.get(song))
								recom_hmap.put(song,p);
						}
					}
				}
				recom_hmap.remove(user_song);								//If the song is already rated, then it is removed from the HashMap
			}
		
			Set<Entry<Integer, Float>> setRecom = recom_hmap.entrySet();
    		List<Entry<Integer, Float>> listRecom = new ArrayList<Entry<Integer, Float>>(setRecom);
    		Collections.sort( listRecom, new Comparator<Map.Entry<Integer, Float>>()
        	{
					public int compare( Map.Entry<Integer, Float> o1, Map.Entry<Integer, Float> o2 )
					{
						return (o2.getValue()).compareTo( o1.getValue() );
					}} );
			String recom = " ";
			int limit = 1;
			for(Map.Entry<Integer, Float> entry:listRecom){							//Storing top 10 recommendations
				limit++;
				recom += Integer.toString(entry.getKey()) + ",";
				if( limit > 10)
					break;
    		} 
			/* to remove all the songs that user had rated from recommendations*/
			String[] temp=sIDRating.split(",");
			ArrayList<String> arr2=new ArrayList<String>();
			for(String str: temp){
				arr2.add(str);		
			}			
			recom = recom.substring(0, recom.length()-1);
			ArrayList<String> arr1=new ArrayList<String>();
			String[] temp1=recom.split(",");
			for(String str1: temp1){
				arr1.add(str1);				
			}
			arr1.removeAll(arr2);			
			System.out.println("final:"+key+"--"+arr1);		
			if(arr1.size()>0)
			context.write(key, new Text(arr1.toString()));
		}
		
		/*
		 * (non-Javadoc)
		 * @see org.apache.hadoop.mapreduce.Reducer#setup(org.apache.hadoop.mapreduce.Reducer.Context)
		 * This setup method is executed at the beginning of the task
		 */
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			Configuration confR = context.getConfiguration();
			String simFileName = confR.get("similaritypath");
			loadingSimData(simFileName);
		}
		
		/*
		 * The following method is used to calculate the predict rating values 
		 * 
		 */
		
		private float predictRating(int itemId, Map<Integer,Integer> userList)
		{
			float sim_RateSum = 0, similaritySum = 0;
			for (Integer item : userList.keySet()) {
				float similarity=0;
				int sID = item;
				int lID = itemId;
				int rating = userList.get(item);				
				if( itemId < sID){
					int t = lID;
					lID = sID;
					sID = t;
				}
				if (SimilarityMap.containsKey(sID)){
					String[] t = SimilarityMap.get(sID).split(",");
    					Map<Integer, Float> prmap = new HashMap<Integer, Float>();
    					for( String str:t){
      						int i = Integer.parseInt(str.split("=")[0]);
      						float f = Float.parseFloat(str.split("=")[1]);
      						prmap.put(i,f);
					}
					if(prmap.containsKey(lID))
						similarity = prmap.get(lID);				 
					else 
						similarity = 0;
				}				
				sim_RateSum += similarity * rating;
				similaritySum += similarity;
			}
			float pR = 0;
			if(similaritySum > 0)
				pR = sim_RateSum/similaritySum;//predicted rating 
			return pR;
		}
		/*
		 * This method will load the similarity data list
		 */
	private void loadingSimData( String simFile) throws FileNotFoundException, IOException {			
			Path pt=new Path(simFile);
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] values = line.split("\t");
				int item = Integer.parseInt(values[0]);		
				SimilarityMap.put(item, values[1]);
			}
			br.close();
		}
	}
}