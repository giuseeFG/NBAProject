package NBA_Project.NBA_Project;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

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
 *  Second Job
 *  Point ranking: top scorer for each season
 *  
 *  e.g.	1) 2011/2012 Durant, Kevin 1824
 *			2) 2006/2007 Bryant, Kobe  2538
 *  		3) . . .
 *  
 *  @input  JSON format  
 *  @author Giuseppe Matrella
 */ 

public class _2_PointRanking2season {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws JSONException {
		long startTime = System.currentTimeMillis();
		JavaSparkContext sc = new JavaSparkContext("local", "Second Job");

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
				String player = "null";
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
					// calculate point value:
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

		// Creating a JSONArray containing the output (it's easier to manage).
		JSONArray outputJsonArray = new JSONArray(outputString);

		// START: managing data and making cleaning operations:
		BidiMap points2playerSeason = new DualHashBidiMap();

		for (int i = 0; i < outputJsonArray.length(); i++) {
			String temp2 = (String) outputJsonArray.get(i);
			String[] tempSplitted = temp2.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String player_season = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				player_season += tempSplitted[j].concat(" ");	
			}

			if (!player_season.contains("null"))
				points2playerSeason.put(Integer.parseInt(value), player_season);
		}

		BidiMap season2playerMax = new DualHashBidiMap();

		for (Object value : points2playerSeason.values()) {
			String[] rawSplitted = String.valueOf(value).split(" ");
			String nameSurname = "";
			for (int j = 0; j < rawSplitted.length-1; j++) {
				nameSurname += rawSplitted[j].concat(" ");	
			}
			String year = rawSplitted[rawSplitted.length-1];
			if (!season2playerMax.containsKey(year)) {
				season2playerMax.put(year, nameSurname.concat(" " + points2playerSeason.getKey(value)));
			}
			else {
				String valueTemp = (String) season2playerMax.get(year);
				String[] valueTempSplitted = valueTemp.split(" ");
				String pointString = valueTempSplitted[valueTempSplitted.length-1];
				Integer val = (Integer) points2playerSeason.getKey(value);
				Integer point = Integer.parseInt(pointString);

				if (val >= point) {
					season2playerMax.put(year, nameSurname.concat(" " + val));
				}
			}
		}

		List<String> finalList = new LinkedList<String>();
		for (Object obj : season2playerMax.keySet()) {
			String key = (String) obj;
			String value = (String) season2playerMax.get(obj);
			finalList.add(key.concat(" ").concat(value));
		}
		// END: managing data and making cleaning operations:

		// Printing raws one by one		
		for (String s : finalList) {
			System.out.println(s);
		}

		sc.stop();
		long estimatedTime = System.currentTimeMillis() - startTime;
		int seconds = (int) (estimatedTime / 1000) ;
		System.out.println("TIME ELAPSED: " + seconds + "s.");
	}
}