package NBA_Project.NBA_Project;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.bson.BSONObject;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import scala.Tuple2;

/**
 *  Total Trend Job
 *  
 *  		. . .
 *  
 *  @input  JSON format  
 *  @author Giuseppe Matrella
 */ 

public class _TotalTrendJob {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws JSONException {

		JavaSparkContext sc = new JavaSparkContext("local", "Total Trend Job");

		Configuration config = new Configuration();
		config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/NBA.fullDB");

		JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);

		/**
		 * This method, applied to "JavaPairRDD<Object, BSONObject>" type object,
		 * read all MongoDB collection (here named "mongoRDD) and, for each document read,
		 * returns all "reports" belong to each match, one by one.
		 * 
		 *   @return Iterable<String>
		 */

		JavaRDD<String> reports = mongoRDD.flatMap(new FlatMapFunction<Tuple2<Object, BSONObject>, String>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Iterable<String> call(Tuple2<Object, BSONObject> arg) throws JSONException {
				BSONObject report = (BSONObject) arg._2.get("report");
				String date = (String) arg._2.get("date");
				JSONArray jArr;
				String reportString = report.toString();
				jArr = new JSONArray(reportString);


				List<String> list = new ArrayList<String>();
				for (int i=0; i<jArr.length(); i++) {
					String temp = jArr.getString(i).replace("}", ",\"date\":\"".concat(date.concat("\"}")));
					list.add(temp);
				}
				return list;
			}
		});

		/**
		 * 
		 * Map function, applied to "JavaRDD<String>" type object,
		 * puts the name of the player who made shot, the season and the relative value.
		 * 
		 * e.g.  	James, LeBron 2008/2009		-	3
		 * 			Durant, Kevin 2010/2011		-	2
		 * 			. . . .
		 * 
		 *   @return Tuple2<String, Integer>
		 *   
		 */

		JavaPairRDD<String, Integer> ones = reports.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) throws JSONException {
				JSONObject obj = new JSONObject(s);
				String player = "";
				int valuePoint = 0;
				String date = obj.getString("date");
				String year = date.substring(0, 4);
				String month = date.substring(4, 6);

				String season = "";

				if (Integer.parseInt(month) < 8)
					season = String.valueOf(Integer.parseInt(year) - 1).concat("/" + year);
				else
					season = (year + "/").concat(String.valueOf(Integer.parseInt(year) + 1));
				if (obj.get("type").equals("point")) {
					String entry = (String) obj.get("entry");
					// Calculating point value:
					if (entry.contains("Free Throw") && entry.contains("PTS"))
						valuePoint = 1;
					else if (entry.contains("3pt") && entry.contains("PTS"))
						valuePoint = 3;
					else if (entry.contains("PTS"))
						valuePoint = 2;
					player = obj.getString("playerName");
				}
				return new Tuple2<>(player.concat(" " + season), valuePoint);
			}
		});

		/**
		 * 
		 * The Reduce function takes the input values,
		 * sums them and generates a single output of 
		 * the word and the final sum.
		 * 
		 * e.g.  	(James, LeBron 2008/2009, 34)
		 * 			(Durant, Kevin 2010/2011, 13)
		 * 			. . . .
		 * 
		 *   @return JavaPairRDD<String, Integer>
		 *   
		 */

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// Create a list of Tuple2<String, Integer> that represents the output.
		List<Tuple2<String, Integer>> output = counts.collect();

		// Cleaning the output putting each tuple in a List<String>
		List<String> outputString = new LinkedList<String>();
		for (Tuple2<String, Integer> tuple : output) {
			outputString.add(tuple._1() + " " + tuple._2());
		}

		Map<Integer, String> points2playerSeason = new TreeMap<Integer, String>(Collections.reverseOrder());

		// Creating a JSONArray containing the output (it's easier to manage).
		JSONArray outputJsonArray = new JSONArray(outputString);

		// START: managing data and making cleaning operations:
		for (int i = 0; i < outputJsonArray.length(); i++) {
			String temp2 = (String) outputJsonArray.get(i);
			String[] tempSplitted = temp2.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String rest = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				rest += tempSplitted[j].concat(" ");	
			}
			if (rest.length() > 15 && Integer.valueOf(value) < 30000)
				points2playerSeason.put(Integer.parseInt(value), rest);
		}
		List<String> finalList = new LinkedList<String>();

		for (Integer i : points2playerSeason.keySet())
			finalList.add(points2playerSeason.get(i).concat(" " + i));

		Map<Integer,String> player2years = new TreeMap<Integer, String>(Collections.reverseOrder());
		BidiMap map06_07 = new DualHashBidiMap();
		BidiMap map07_08 = new DualHashBidiMap();
		BidiMap map08_09 = new DualHashBidiMap();
		BidiMap map09_10 = new DualHashBidiMap();
		BidiMap map10_11 = new DualHashBidiMap();
		BidiMap map11_12 = new DualHashBidiMap();

		for (Integer points1 : points2playerSeason.keySet()) {
			if ( points2playerSeason.get(points1).contains("2006/2007"))
				map06_07.put(points1, points2playerSeason.get(points1));
			else if ( points2playerSeason.get(points1).contains("2007/2008"))
				map07_08.put(points1, points2playerSeason.get(points1));
			else if ( points2playerSeason.get(points1).contains("2008/2009"))
				map08_09.put(points1, points2playerSeason.get(points1));
			else if ( points2playerSeason.get(points1).contains("2009/2010"))
				map09_10.put(points1, points2playerSeason.get(points1));
			else if ( points2playerSeason.get(points1).contains("2010/2011"))
				map10_11.put(points1, points2playerSeason.get(points1));
			else if ( points2playerSeason.get(points1).contains("2011/2012"))
				map11_12.put(points1, points2playerSeason.get(points1));
		}

		List<BidiMap> listMap = new LinkedList<BidiMap>();

		listMap.add(map06_07);
		listMap.add(map07_08);
		listMap.add(map08_09);
		listMap.add(map09_10);
		listMap.add(map10_11);
		listMap.add(map11_12);

		for (int i = 0; i < listMap.size()-1; i++) {
			BidiMap actualMap = listMap.get(i);
			for (Object points : actualMap.keySet()) {
				String value = (String) actualMap.get(points);
				String[] valueSplitted = value.split(" ");
				String year = value.substring(value.length()-10, value.length());
				String name = "";
				for (int j = 0; j < valueSplitted.length-1; j++) 
					name += valueSplitted[j].concat(" ");
				BidiMap nextMap = listMap.get(i+1);
				for (Object points2 : nextMap.keySet()) {
					String value2 = (String) nextMap.get(points2);
					if (value2.contains(name)) {
						if(name.length() < 15)
							name += "\t";
						Integer difference = (Integer)points2 - (Integer)points;
						String year2 = value2.substring(value.length()-10, value.length());
						player2years.put(difference, name + "\tmade " + difference + " points more between " + year + "and " + year2);
					}
				}
			}
		}

		List<String> listDiffPoints = new LinkedList<String>();
		for (String s : player2years.values()) {
			listDiffPoints.add(s);
		}
		// END: managing data and making cleaning operations
		
		// Printing 10 raws
		for (int i = 0; i < 11; i++)
			System.out.println(i+1 + " " + listDiffPoints.get(i));

		sc.stop();
	}
}