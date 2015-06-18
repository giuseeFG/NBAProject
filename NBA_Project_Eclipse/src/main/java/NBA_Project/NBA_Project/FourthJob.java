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

public class FourthJob {
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
				String date = obj.getString("date");
				String anno = date.substring(0, 4);
				String mese = date.substring(4, 6);

				String stagione = "";

				if (Integer.parseInt(mese) < 8)
					stagione = String.valueOf(Integer.parseInt(anno) - 1).concat("/" + anno);
				else
					stagione = (anno + "/").concat(String.valueOf(Integer.parseInt(anno) + 1));
				try {
					String entry = (String) obj.get("entry");
					if (entry.contains("PF)")) {
						String[] eventDetails = entry.split("] ");
						try {
							
							String[] name = eventDetails[1].split(" ");
							if (name[0].contains(".") && name[1].contains(".")) 
								player = name[0].concat(name[1]).concat(name[2]);
							else if (name[0].contains(".")) 
								player = name[0].concat(name[1]);							
							else 
								player = name[0];
						}
						catch(Exception e) {
							System.out.println("errore " + s);
						}
					}
				}
				catch(Exception ee) {
					System.out.println("ERRORE " + s);
				}
				return new Tuple2<>(player.concat(" " + stagione), 1);
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

		String temp = output.toString().replace("(","\"");
		temp = temp.replace(")","\"");
		JSONArray outputJsonArray = new JSONArray(temp);

		System.out.println("falli: " + outputJsonArray);

		BidiMap fouls2playerSeason = new DualHashBidiMap();

		for (int i = 0; i < outputJsonArray.length(); i++) {
			String temp2 = (String) outputJsonArray.get(i);
			temp2.replace(" ", "\t");
			temp2.replace(",", "\t");
			String[] splitted = temp2.split(",");
			if (Integer.valueOf(splitted[1]) < 30000) {
				// vuol dire che ha beccato il caso in cui c'è la stringa vuota con accanto un numero gigante
				fouls2playerSeason.put(Integer.valueOf(splitted[1]), splitted[0]);
			}
		}

		System.out.println("falli: " + fouls2playerSeason);		

		BidiMap season2playerMax = new DualHashBidiMap();

		for (Object value : fouls2playerSeason.values()) {
			String[] rawSplitted = String.valueOf(value).split(" ");
			String year = rawSplitted[1];
			if (!season2playerMax.containsKey(year)) {
				season2playerMax.put(year, rawSplitted[0].concat(" " + fouls2playerSeason.getKey(value)));
			}
			else {
				String valueTemp = (String) season2playerMax.get(year);
				String[] valueTempSplitted = valueTemp.split(" ");
				String point = valueTempSplitted[1];
				if ((int)fouls2playerSeason.getKey(value) > Integer.parseInt(point)) {
					season2playerMax.put(year, rawSplitted[0].concat(" " + fouls2playerSeason.getKey(value)));
				}
			}
		}

		System.out.println(season2playerMax);
		List<String> finalList = new LinkedList<String>();

		for (Object obj : season2playerMax.keySet()) {
			System.out.println("OBJ " + (String) obj);
			System.out.println("value " + (String) season2playerMax.get(obj));
			finalList.add((String) obj + " " + (String) season2playerMax.get(obj));
		}

		System.out.println("FINALE " + finalList);
		
		for (int i = 0; i < finalList.size(); i++) {
			System.out.println(i+1 + " " + finalList.get(i));
			
		}

		sc.stop();
	}
}