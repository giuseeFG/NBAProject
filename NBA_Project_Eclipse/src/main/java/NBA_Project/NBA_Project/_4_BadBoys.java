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
 *  Fourth Job
 *  Bad boys ranking: top personal foul for each season
 *  
 *  e.g.	1 2011/2012 Perkins, Kendrick	197
 *			2 2006/2007 Stoudemire, Amare  	358
 *			3 2009/2010 Howard, Dwight  	368
 *			4 2007/2008 Perkins, Kendrick  	363
 *			. . . .
 *  
 *  @input  JSON format  
 *  @author Giuseppe Matrella
 */

public class _4_BadBoys {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws JSONException {
		long startTime = System.currentTimeMillis();
		JavaSparkContext sc = new JavaSparkContext("local", "Fourth Job");

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
		 * puts the name of the player who made a personal foul, the season and "1".
		 * 
		 * e.g.  	James, LeBron 2008/2009		-	1
		 * 			Durant, Kevin 2010/2011		-	1
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
				String date = obj.getString("date");
				String year = date.substring(0, 4);
				String month = date.substring(4, 6);

				String season = "";

				if (Integer.parseInt(month) < 8)
					season = String.valueOf(Integer.parseInt(year) - 1).concat("/" + year);
				else
					season = (year + "/").concat(String.valueOf(Integer.parseInt(year) + 1));
				try {
					String entry = (String) obj.get("entry");
					if (entry.contains("PF)"))
						player = obj.getString("playerName"); 
				}
				catch(Exception ee) {
					System.out.println("Error " + s);
				}
				return new Tuple2<>(player.concat(" " + season), 1);
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
		BidiMap fouls2playerSeason = new DualHashBidiMap();

		for (int i = 0; i < outputJsonArray.length(); i++) {
			String temp2 = (String) outputJsonArray.get(i);
			String[] tempSplitted = temp2.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String name_season = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				name_season += tempSplitted[j].concat(" ");	
			}
			if (!name_season.contains("null")) {
				fouls2playerSeason.put(value, name_season);
			}
		}

		BidiMap season2playerMax = new DualHashBidiMap();

		for (Object value : fouls2playerSeason.values()) {
			String[] rawSplitted = String.valueOf(value).split(" ");
			String season = rawSplitted[rawSplitted.length-1];
			String nameSurname = "";
			for (int j = 0; j < rawSplitted.length-1; j++) {
				nameSurname += rawSplitted[j].concat(" ");	
			}
			if (nameSurname.length() > 5) {
				if (!season2playerMax.containsKey(season)) {
					season2playerMax.put(season, nameSurname.concat(" " + fouls2playerSeason.getKey(value)));
				}
				else {
					String valueTemp = (String) season2playerMax.get(season);
					String[] valueTempSplitted = valueTemp.split(" ");
					String point = valueTempSplitted[valueTempSplitted.length-1];
					if (Integer.parseInt((String)fouls2playerSeason.getKey(value)) > Integer.parseInt(point)) {
						season2playerMax.put(season, nameSurname.concat(" " + fouls2playerSeason.getKey(value)));
					}
				}
			}
		}

		List<String> finalList = new LinkedList<String>();

		for (Object obj : season2playerMax.keySet()) {
			finalList.add((String) obj + " " + (String) season2playerMax.get(obj));
		}

		// END: managing data and making cleaning operations:

		// Printing raws one by one (only 10)
		for (int i = 0; i < finalList.size(); i++) {
			System.out.println(i+1 + " " + finalList.get(i));
		}

		sc.stop();
		long estimatedTime = System.currentTimeMillis() - startTime;
		int seconds = (int) (estimatedTime / 1000) ;
		System.out.println("TIME ELAPSED: " + seconds + "s.");
	}
}