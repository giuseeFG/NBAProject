package NBA_Project.NBA_Project;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.SortedBidiMap;
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
 *  Seventh Job
 *  Buzzer beater: top 3 each season.
 *  
 *  e.g.	Bryant, Kobe 2008/2009  	2842
 *  		James, LeBron 2008/2009 	2798
 *  		Durant, Kevin 2010/2011  	2648
 *  		. . .
 *  
 *  @input  JSON format  
 *  @author Giuseppe Matrella
 */ 

public class _7Job {
	@SuppressWarnings("resource")
	public static void main(String[] args) throws JSONException {

		JavaSparkContext sc = new JavaSparkContext("local", "Seventh Job");

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
		 * puts the name of the player who made shot in the lasts 12 seconds of a match.
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
				String date = obj.getString("date");
				int valuePoint = 0;
				String timeRemaining = obj.getString("timeRemaining");
				int secondsRemaining = 50, minuteRemaining = 50;
				try {
					secondsRemaining = Integer.parseInt(timeRemaining.split(":")[2]);
					minuteRemaining = Integer.parseInt(timeRemaining.split(":")[1]);
				}
				catch (Exception e){}
				String year = date.substring(0, 4);
				String month = date.substring(4, 6);

				String season = "";

				if (Integer.parseInt(month) < 8)
					season = String.valueOf(Integer.parseInt(year) - 1).concat("/" + year);
				else
					season = (year + "/").concat(String.valueOf(Integer.parseInt(year) + 1));

				if (obj.get("type").equals("point") && secondsRemaining <= 12 && minuteRemaining == 0) {
					String entryOld = (String) obj.get("entry");
					player = obj.getString("playerName");
					// controllo se è effettivamente un PUNTO, ovvero un canestro, a prescindere dal valore del canestro.
					// se è un tiro libero lo considero 0 (perché i tiri liberi non li consideriamo). 
					if (entryOld.contains("Free Throw") && entryOld.contains("PTS")) 
						valuePoint = 0;
					else if (entryOld.contains("3pt") && entryOld.contains("PTS"))
						valuePoint = 1;
					else if (entryOld.contains("PTS"))
						valuePoint = 1;
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

		JavaPairRDD<String, Integer> countsBuzzerBeater = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		// Create a list of Tuple2<String, Integer> that represents the output.
		List<Tuple2<String, Integer>> outputBuzzerBeater = countsBuzzerBeater.collect();

		// Cleaning the output putting each tuple in a List<String>
		List<String> outputString = new LinkedList<String>();

		for (Tuple2<String, Integer> tuple : outputBuzzerBeater) {
			outputString.add(tuple._1() + " " + tuple._2());
		}

		// Creating a JSONArray containing the output (it's easier to manage).
		JSONArray outputBuzzerBeaterJsonArray = new JSONArray(outputString);

		// START: managing data and making cleaning operations:
		Map<String, String> player2points = new HashMap<String, String>();

		for (int i = 0; i < outputBuzzerBeaterJsonArray.length(); i++) {
			String temp = outputBuzzerBeaterJsonArray.getString(i);
			String[] tempSplitted = temp.split(" "); 
			String value = tempSplitted[tempSplitted.length-1];
			String rest = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				rest += tempSplitted[j].concat(" ");	
			}
			if (rest.length() > 15)
				player2points.put(rest, value);
		}

		SortedBidiMap map06_07 = new DualTreeBidiMap();
		SortedBidiMap map07_08 = new DualTreeBidiMap();
		SortedBidiMap map08_09 = new DualTreeBidiMap();
		SortedBidiMap map09_10 = new DualTreeBidiMap();
		SortedBidiMap map10_11 = new DualTreeBidiMap();
		SortedBidiMap map11_12 = new DualTreeBidiMap();

		for (Object s : player2points.keySet()) {
			if (((String) s).contains("2006/2007")){
				double valore = Double.valueOf(player2points.get(s))*(-1);
				map06_07.put(valore, s);
			}
			else if (((String) s).contains("2007/2008")){
				double valore = Double.valueOf(player2points.get(s))*(-1);
				map07_08.put(valore, s);
			}
			else if (((String) s).contains("2008/2009")){
				double valore = Double.valueOf(player2points.get(s))*(-1);
				map08_09.put(valore, s);
			}
			else if (((String) s).contains("2009/2010")){
				double valore = Double.valueOf(player2points.get(s))*(-1);
				map09_10.put(valore, s);
			}
			else if (((String) s).contains("2010/2011")){
				double valore = Double.valueOf(player2points.get(s))*(-1);
				map10_11.put(valore, s);
			}
			else if (((String) s).contains("2011/2012")){
				double valore = Double.valueOf(player2points.get(s))*(-1);
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
		
		// Printing raws one by one, 3 for each season
		for (LinkedList<String> list : listaFinale) {
			for (int i = 0; i < 3; i++) {
				String[] array = list.toArray(new String[list.size()]);
				System.out.println(i+1 + " " + array[i]);
			}
			System.out.println("\n");
			System.out.println("--------------------");
			System.out.println("\n");
		}


		sc.stop();
	}
}