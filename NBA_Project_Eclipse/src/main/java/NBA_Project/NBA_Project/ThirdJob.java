package NBA_Project.NBA_Project;

/*
 * JavaWordCount.java
 * Written in 2014 by Sampo Niskanen / Mobile Wellness Solutions MWS Ltd
 * 
 * To the extent possible under law, the author(s) have dedicated all copyright and
 * related and neighboring rights to this software to the public domain worldwide.
 * This software is distributed without any warranty.
 * 
 * See <http://creativecommons.org/publicdomain/zero/1.0/> for full details.
 */
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

public class ThirdJob {
	public static void main(String[] args) throws JSONException {

		JavaSparkContext sc = new JavaSparkContext("local", "First Job - Point Ranking");

		Configuration config = new Configuration();
		config.set("mongo.input.uri", "mongodb://127.0.0.1:27017/NBA.fullDB");

		JavaPairRDD<Object, BSONObject> mongoRDD = sc.newAPIHadoopRDD(config, com.mongodb.hadoop.MongoInputFormat.class, Object.class, BSONObject.class);

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

		// primo mapper: mi mappo i giocatori e i tiri "missed"
		JavaPairRDD<String, Integer> missedMap = reports.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) throws JSONException {
				JSONObject obj = new JSONObject(s);
				String player = "";

				String date = obj.getString("date");
				String anno = date.substring(0, 4);
				String mese = date.substring(4, 6);

				String stagione = "";

				if (Integer.parseInt(mese) < 8)
					stagione = String.valueOf(Integer.parseInt(anno) - 1).concat("/" + anno);
				else
					stagione = (anno + "/").concat(String.valueOf(Integer.parseInt(anno) + 1));
				String entry = "";
				try {
					entry = (String) obj.get("entry");
				}
				catch (Exception ee) {
				}
				if (entry.contains("Missed")) {
					player = obj.getString("playerName");
				}
				return new Tuple2<>(player.concat(" " + stagione), 1);
			}
		});

		//secondo mapper: mi mappo i giocatori e i canestri
		JavaPairRDD<String, Integer> pointsMap = reports.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) throws JSONException {
				JSONObject obj = new JSONObject(s);
				String player = "";
				String date = obj.getString("date");
				String anno = date.substring(0, 4);
				String mese = date.substring(4, 6);
				int valuePoint = 0;
				String stagione = "";

				if (Integer.parseInt(mese) < 8)
					stagione = String.valueOf(Integer.parseInt(anno) - 1).concat("/" + anno);
				else
					stagione = (anno + "/").concat(String.valueOf(Integer.parseInt(anno) + 1));

				if (obj.get("type").equals("point")) {
					String entryOld = (String) obj.get("entry");
					//controllo se è effettivamente un PUNTO, ovvero un canestro, a prescindere dal valore del canestro.
					if (entryOld.contains("Free Throw") && entryOld.contains("PTS"))
						valuePoint = 1;
					else if (entryOld.contains("3pt") && entryOld.contains("PTS"))
						valuePoint = 1;
					else if (entryOld.contains("PTS"))
						valuePoint = 1;

					player = obj.getString("playerName");
				}
				return new Tuple2<>(player.concat(" " + stagione), valuePoint);
			}
		});




		JavaPairRDD<String, Integer> countsMissed = missedMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		JavaPairRDD<String, Integer> countsPoints = pointsMap.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> outputMissed = countsMissed.collect();
		List<Tuple2<String, Integer>> outputPoints = countsPoints.collect();

		List<String> outputMissedString = new LinkedList<String>();
		List<String> outputPointsString = new LinkedList<String>();

		for (Tuple2<String, Integer> tuple : outputMissed) {
			outputMissedString.add(tuple._1() + " " + tuple._2());
		}

		for (Tuple2<String, Integer> tuple : outputPoints) {
			outputPointsString.add(tuple._1() + " " + tuple._2());
		}

		JSONArray outputMissedJsonArray = new JSONArray(outputMissedString);
		JSONArray outputPointJsonArray = new JSONArray(outputPointsString);

		Map<String, String> player2misses = new HashMap<String, String>();
		Map<String, String> player2points = new HashMap<String, String>();

		for (int i = 0; i < outputMissedJsonArray.length(); i++) {
			String temp = outputMissedJsonArray.getString(i);
			String[] tempSplitted = temp.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String rest = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				rest += tempSplitted[j].concat(" ");	
			}
			player2misses.put(rest, value);
		}
		for (int i = 0; i < outputPointJsonArray.length(); i++) {
			String temp = outputPointJsonArray.getString(i);
			String[] tempSplitted = temp.split(" ");
			String value = tempSplitted[tempSplitted.length-1];
			String rest = "";
			for (int j = 0; j < tempSplitted.length-1; j++) {
				rest += tempSplitted[j].concat(" ");	
			}
			player2points.put(rest, value);
		}

		BidiMap finalMap = new DualHashBidiMap();

		for (String s : player2misses.keySet()) {
			for (String s2 : player2points.keySet()) {
				if (s.equals(s2)) {

					double sum = Double.parseDouble(player2points.get(s)) + Integer.parseInt(player2misses.get(s));
					double missed = Double.parseDouble(player2points.get(s));
					double ratio = missed/sum;

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
				if (((String) s).length() > 15)
					map06_07.put(valore, s);
			}
			else if (((String) s).contains("2007/2008")){
				double valore = (double) finalMap.get(s)*(-1);
				if (((String) s).length() > 15)
					map07_08.put(valore, s);
			}
			else if (((String) s).contains("2008/2009")){
				double valore = (double) finalMap.get(s)*(-1);
				if (((String) s).length() > 15)
					map08_09.put(valore, s);
			}
			else if (((String) s).contains("2009/2010")){
				double valore = (double) finalMap.get(s)*(-1);
				if (((String) s).length() > 15)
					map09_10.put(valore, s);
			}
			else if (((String) s).contains("2010/2011")){
				double valore = (double) finalMap.get(s)*(-1);
				if (((String) s).length() > 15)
					map10_11.put(valore, s);
			}
			else if (((String) s).contains("2011/2012")){
				double valore = (double) finalMap.get(s)*(-1);
				if (((String) s).length() > 15)
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