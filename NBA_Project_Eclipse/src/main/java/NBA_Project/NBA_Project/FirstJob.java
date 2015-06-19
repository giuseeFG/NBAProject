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
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

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

public class FirstJob {
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

		JavaPairRDD<String, Integer> ones = reports.mapToPair(new PairFunction<String, String, Integer>() {
			private static final long serialVersionUID = 1L;

			public Tuple2<String, Integer> call(String s) throws JSONException {
				JSONObject obj = new JSONObject(s);
				String player = "";
				int valuePoint = 0;
				String date = obj.getString("date");
				String anno = date.substring(0, 4);
				String mese = date.substring(4, 6);

				String stagione = "";

				if (Integer.parseInt(mese) < 8)
					stagione = String.valueOf(Integer.parseInt(anno) - 1).concat("/" + anno);
				else
					stagione = (anno + "/").concat(String.valueOf(Integer.parseInt(anno) + 1));

				if (obj.get("type").equals("point")) {
					String entry = (String) obj.get("entry");
					//calculate point value:
					if (entry.contains("Free Throw") && entry.contains("PTS"))
						valuePoint = 1;
					else if (entry.contains("3pt") && entry.contains("PTS"))
						valuePoint = 3;
					else if (entry.contains("PTS"))
						valuePoint = 2;
					player = obj.getString("playerName");
				}
				return new Tuple2<>(player.concat(" " + stagione), valuePoint);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = counts.collect();
		List<String> outputString = new LinkedList<String>();
		for (Tuple2<String, Integer> tuple : output) {
			outputString.add(tuple._1() + " " + tuple._2());
		}

		JSONArray outputJsonArray = new JSONArray(outputString);

		Map<Integer, String> points2playerSeason = new TreeMap<Integer, String>(Collections.reverseOrder());

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
		//PRINT FINAL LIST
		for (int i = 1; i < 11; i++) {
			System.out.println(i + " " + finalList.get(i) + "\n");
		}
		sc.stop();
	}
}