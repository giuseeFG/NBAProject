package NBA_Project.NBA_Project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.BidiMap;
import org.apache.commons.collections.SortedBidiMap;
import org.apache.commons.collections.bidimap.DualHashBidiMap;
import org.apache.commons.collections.bidimap.DualTreeBidiMap;
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
 *  Third Job
 *  Point ratio: top 3 performer, each season.
 *  
 *  e.g.	1 0.76 Baston, Maceo		2006/2007 
 *			2 0.76 Henderson, Alan 		2006/2007 
 *			3 0.73 Outlaw, Bo 			2006/2007 
 *			-------------------------------------
 *			1 0.75 Baston, Maceo 		2007/2008 
 *			2 0.73 Howard, Dwight	 	2007/2008 
 *			3 0.71 Bynum, Andrew 		2007/2008 
 *			-------------------------------------
 *			1 0.82 Sene, Mouhamed 		2008/2009 
 *			2 0.72 Jones, Dwayne 		2008/2009 
 *			3 0.72 Jordan, DeAndre	 	2008/2009
 *			------------------------------------- 
 *			. . . . . . .
 *			. . . . . . .
 *
 *  
 *  @input  JSON format  
 *  @author Giuseppe Matrella
 */ 

public class _3_PointRatio {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws JSONException {
		long startTime = System.currentTimeMillis();
		JavaSparkContext sc = new JavaSparkContext("local", "Third Job");

		Configuration config = new Configuration();
		config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/NBA.fullDB_new");

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
		 * FIRST Map function, applied to "JavaRDD<String>" type object,
		 * puts the name of the player who missed a shot.
		 * 
		 * e.g.  	James, LeBron 2008/2009		-	1
		 * 			Durant, Kevin 2010/2011		-	1
		 * 			. . . .
		 * 
		 *   @return Tuple2<String, Integer>
		 *   
		 */

		JavaPairRDD<String, Integer> missedMap = reports.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) throws JSONException {
				JSONObject obj = new JSONObject(s);
				String player = "null";

				String date = obj.getString("date");
				String year = date.substring(0, 4);
				String month = date.substring(4, 6);

				String season = "";

				if (Integer.parseInt(month) < 8)
					season = String.valueOf(Integer.parseInt(year) - 1).concat("/" + year);
				else
					season = (year + "/").concat(String.valueOf(Integer.parseInt(year) + 1));

				if (obj.get("type").equals("generalEvent")) {
					String entry = (String) obj.get("entry");
					if (entry.contains("Missed") || entry.contains("missed")) {
						player = obj.getString("playerName");
					}
				}
				return new Tuple2<>(player.concat(" " + season), 1);
			}
		});

		/**
		 * 
		 * SECOND Map function, applied to "JavaRDD<String>" type object,
		 * puts the name of the player who made a shot. I don't care about the value.
		 * 
		 * e.g.  	James, LeBron 2008/2009		-	1
		 * 			Durant, Kevin 2010/2011		-	1
		 * 			. . . .
		 * 
		 *   @return Tuple2<String, Integer>
		 *   
		 */

		JavaPairRDD<String, Integer> pointsMap = reports.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) throws JSONException {
				JSONObject obj = new JSONObject(s);
				String player = "null";
				String date = obj.getString("date");
				String year = date.substring(0, 4);
				String month = date.substring(4, 6);
				int valuePoint = 0;
				String season = "";

				if (Integer.parseInt(month) < 8)
					season = String.valueOf(Integer.parseInt(year) - 1).concat("/" + year);
				else
					season = (year + "/").concat(String.valueOf(Integer.parseInt(year) + 1));

				if (obj.get("type").equals("point")) {
					String entryOld = (String) obj.get("entry");
					// checking if it's actually a made point, I don't care about the value. 
					if (entryOld.contains("Free Throw") && entryOld.contains("PTS"))
						valuePoint = 1;
					else if (entryOld.contains("3pt") && entryOld.contains("PTS"))
						valuePoint = 1;
					else if (entryOld.contains("PTS"))
						valuePoint = 1;
					player = obj.getString("playerName");
				}
				return new Tuple2<>(player.concat(" " + season), valuePoint);
			}
		});

		/**
		 * 
		 * The Reduce function (missed shots) takes the input values,
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

		JavaPairRDD<String, Integer> countsMissed = missedMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		/**
		 * 
		 * The Reduce function (made shots) takes the input values,
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

		JavaPairRDD<String, Integer> countsPoints = pointsMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});
		// Create a list of Tuple2<String, Integer> that represents the output (missed shots).
		List<Tuple2<String, Integer>> outputMissed = countsMissed.collect();
		// Create a list of Tuple2<String, Integer> that represents the output (made shots).
		List<Tuple2<String, Integer>> outputPoints = countsPoints.collect();

		// Cleaning the output putting each tuple of each output (missed and made shots) in a List<String>
		List<String> outputMissedString = new LinkedList<String>();
		List<String> outputPointsString = new LinkedList<String>();

		for (Tuple2<String, Integer> tuple : outputMissed) {
			outputMissedString.add(tuple._1() + " " + tuple._2());
		}

		for (Tuple2<String, Integer> tuple : outputPoints) {
			outputPointsString.add(tuple._1() + " " + tuple._2());
		}

		//System.out.println(outputMissedString);
		//System.out.println(outputPointsString);

		// Creating a JSONArray containing the output (missed and made shots - it's easier to manage JSONArray type).
		JSONArray outputMissedJsonArray = new JSONArray(outputMissedString);
		JSONArray outputPointJsonArray = new JSONArray(outputPointsString);

		// START: managing data and making cleaning operations:
		Map<String, String> player2misses = new HashMap<String, String>();
		Map<String, String> player2points = new HashMap<String, String>();

		for (int i = 0; i < outputMissedJsonArray.length(); i++) {
			String temp = outputMissedJsonArray.getString(i);
			String[] tempSplitted = temp.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String name_season = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				name_season += tempSplitted[j].concat(" ");	
			}
			if (!name_season.contains("null"))
				player2misses.put(name_season, value);
		}
		for (int i = 0; i < outputPointJsonArray.length(); i++) {
			String temp = outputPointJsonArray.getString(i);
			String[] tempSplitted = temp.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String name_season = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				name_season += tempSplitted[j].concat(" ");	
			}
			if (!name_season.contains("null"))
				player2points.put(name_season, value);
		}

		BidiMap finalMap = new DualHashBidiMap();

		for (String s : player2misses.keySet()) {
			for (String s2 : player2points.keySet()) {
				if (s.equals(s2)) {

					double sum = Double.parseDouble(player2points.get(s)) + Integer.parseInt(player2misses.get(s));
					double missed = Double.parseDouble(player2points.get(s));
					double ratio = missed/sum;
					
					if (sum > 300)
						finalMap.put(s, ratio);
				}
			}
		}

		SortedBidiMap map06_07 = new DualTreeBidiMap();
		SortedBidiMap map07_08 = new DualTreeBidiMap();
		SortedBidiMap map08_09 = new DualTreeBidiMap();
		SortedBidiMap map09_10 = new DualTreeBidiMap();
		SortedBidiMap map10_11 = new DualTreeBidiMap();
		SortedBidiMap map11_12 = new DualTreeBidiMap();

		for (Object s : finalMap.keySet()) {
			if (((String) s).contains("2006/2007")){
				double valore = (double) finalMap.get(s)*(-1);
				//if (((String) s).length() > 15)
				map06_07.put(valore, s);
			}
			else if (((String) s).contains("2007/2008")){
				double valore = (double) finalMap.get(s)*(-1);
				//if (((String) s).length() > 15) 
				map07_08.put(valore, s);
			}
			else if (((String) s).contains("2008/2009")){
				double valore = (double) finalMap.get(s)*(-1);
				//if (((String) s).length() > 15)
				map08_09.put(valore, s);
			}
			else if (((String) s).contains("2009/2010")){
				double valore = (double) finalMap.get(s)*(-1);
				//if (((String) s).length() > 15)
				map09_10.put(valore, s);
			}
			else if (((String) s).contains("2010/2011")){
				double valore = (double) finalMap.get(s)*(-1);
				//if (((String) s).length() > 15)
				map10_11.put(valore, s);
			}
			else if (((String) s).contains("2011/2012")){
				double valore = (double) finalMap.get(s)*(-1);
				//if (((String) s).length() > 15)
				map11_12.put(valore, s);
			}
		}

		List<LinkedList<String>> listaFinale = new LinkedList<LinkedList<String>>();

		List<String> lista = new LinkedList<String>();

		for (Object obj : map06_07.keySet()) {
			lista.add(String.valueOf(Double.parseDouble(String.valueOf(obj))*(-1)) + " " + map06_07.get(obj));
		}
		listaFinale.add((LinkedList<String>) lista);
		lista = new LinkedList<String>();
		for (Object obj : map07_08.keySet()) {
			lista.add(String.valueOf(Double.parseDouble(String.valueOf(obj))*(-1)) + " " + map07_08.get(obj));
		}
		listaFinale.add((LinkedList<String>) lista);
		lista = new LinkedList<String>();
		for (Object obj : map08_09.keySet()) {
			lista.add(String.valueOf(Double.parseDouble(String.valueOf(obj))*(-1)) + " " + map08_09.get(obj));
		}
		listaFinale.add((LinkedList<String>) lista);
		lista = new LinkedList<String>();
		for (Object obj : map09_10.keySet()) {
			lista.add(String.valueOf(Double.parseDouble(String.valueOf(obj))*(-1)) + " " + map09_10.get(obj));
		}
		listaFinale.add((LinkedList<String>) lista);
		lista = new LinkedList<String>();
		for (Object obj : map10_11.keySet()) {
			lista.add(String.valueOf(Double.parseDouble(String.valueOf(obj))*(-1)) + " " + map10_11.get(obj));
		}
		listaFinale.add((LinkedList<String>) lista);
		lista = new LinkedList<String>();
		for (Object obj : map11_12.keySet()) {
			lista.add(String.valueOf(Double.parseDouble(String.valueOf(obj))*(-1)) + " " + map11_12.get(obj));
		}
		listaFinale.add((LinkedList<String>) lista);

		// END: managing data and making cleaning operations:

		// Printing raws one by one, 3 for each season.
		System.out.println("LISTA FINALE " + listaFinale);
		for (LinkedList<String> list : listaFinale) {
			for (int i = 0; i < 3; i++) {
				String[] array = list.toArray(new String[list.size()]);
				System.out.println(i+1 + " " + array[i]);
			}
			System.out.println("--------------------");
		}

		sc.stop();
		long estimatedTime = System.currentTimeMillis() - startTime;
		int seconds = (int) (estimatedTime / 1000) ;
		System.out.println("TIME ELAPSED: " + seconds + "s.");
	}
}