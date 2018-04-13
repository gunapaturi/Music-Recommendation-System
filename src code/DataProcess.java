package com.uncc.cloudProject;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * This class will process the data set. Data set contains UID, SID and Rating values
 */

public class DataProcess
{

	/*
	 * Mapper Class: Map_DataProcess
	 * Input is UID, SID and Rating
	 * Output is UID(key), (SID, Rating)(Value)
	 */
	public static class Map_DataProcess extends Mapper<LongWritable ,  Text ,  Text ,  Text > {

		public void map( LongWritable offset,  Text lineText,  Context context)
				throws  IOException,  InterruptedException {

			String line  = lineText.toString();

			String[] parts = StringUtils.split(line);
			if(parts.length == 3){
				String userID = parts[0];
				String songID = parts[1];
				String rating = parts[2];

				context.write(new Text(userID), new Text(songID + "-->" + rating));
			}
		}
	}
	/*
	 * Reducer Class: Reduce_DataProcess
	 * Input is UID(key), (SID, Rating)(Value)
	 * Output is UID(key), (SID-->Rating)(Value), (SID-->Rating)
	 */
	public static class Reduce_DataProcess extends Reducer<Text ,  Text ,  Text ,  Text > {
		@Override 
		public void reduce( Text userID,  Iterable<Text > values,  Context context)
				throws IOException,  InterruptedException {

			String songID_rating = "";
			for ( Text val  : values) {
				songID_rating = songID_rating + "," + val.toString();
			}

			context.write(userID,  new Text(songID_rating));
		}
	}

	
}
