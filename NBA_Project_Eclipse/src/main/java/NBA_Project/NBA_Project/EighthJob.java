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

public class EighthJob {
	public static void main(String[] args) throws JSONException {

		JavaSparkContext sc = new JavaSparkContext("local", "Eidhth Job - Point Ranking: top 3, each quart, all season");

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
				String player = obj.getString("playerName");
				String date = obj.getString("date");
				int valuePoint = 0;
				String timeRemaining = obj.getString("timeRemaining");
				int minuteRemaining = 100;
				try {
					minuteRemaining = Integer.parseInt(timeRemaining.split(":")[1]);
				}
				catch (Exception e){}
				String quarto = "0";
				if (timeRemaining.contains("-"))
					quarto = "OverTime";
				else if (minuteRemaining <= 12 && minuteRemaining >= 0)
					quarto = "4TH";
				else if (minuteRemaining <= 24 && minuteRemaining > 12)
					quarto = "3RD";
				else if (minuteRemaining <= 36 && minuteRemaining > 24)
					quarto = "2ND";
				else if (minuteRemaining <= 48 && minuteRemaining > 36)
					quarto = "1ST";

				String anno = date.substring(0, 4);
				String mese = date.substring(4, 6);

				String stagione = "";

				if (Integer.parseInt(mese) < 8)
					stagione = String.valueOf(Integer.parseInt(anno) - 1).concat("/" + anno);
				else
					stagione = (anno + "/").concat(String.valueOf(Integer.parseInt(anno) + 1));

				if (obj.get("type").equals("point")) {
					String entryOld = (String) obj.get("entry");
					if (entryOld.contains("Free Throw") && entryOld.contains("PTS")) 
						valuePoint = 1;
					else if (entryOld.contains("3pt") && entryOld.contains("PTS"))
						valuePoint = 3;
					else if (entryOld.contains("PTS"))
						valuePoint = 2;

				}
				return new Tuple2<>(quarto.concat(" " + player.concat(" " + stagione)), valuePoint);
			}
		});

		JavaPairRDD<String, Integer> countsPointsQuart = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = 1L;
			@Override
			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		List<Tuple2<String, Integer>> output = countsPointsQuart.collect();

		System.out.println("ASDAS " + output.toString());
		String temp = output.toString().replace("(","\"");
		temp = temp.replace(")","\"");
		JSONArray outputJsonArray = new JSONArray(temp);

		BidiMap finalMap = new DualHashBidiMap();

		for (int i = 0; i < outputJsonArray.length(); i++) {
			String tempString = outputJsonArray.getString(i);
			try {
				finalMap.put(Integer.parseInt(tempString.split(",")[2]), tempString.split(",")[0]+tempString.split(",")[1]);
			}
			catch (Exception e) {
			}
		}

		SortedBidiMap map06_07 = new DualTreeBidiMap();
		SortedBidiMap map07_08 = new DualTreeBidiMap();
		SortedBidiMap map08_09 = new DualTreeBidiMap();
		SortedBidiMap map09_10 = new DualTreeBidiMap();
		SortedBidiMap map10_11 = new DualTreeBidiMap();
		SortedBidiMap map11_12 = new DualTreeBidiMap();

		for (Object s : finalMap.values()) {
			if (((String) s).contains("2006/2007")){
				Integer valore = (Integer) finalMap.getKey(s)*(-1);
				map06_07.put(valore, s);
			}
			else if (((String) s).contains("2007/2008")){
				Integer valore = (Integer) finalMap.getKey(s)*(-1);
				map07_08.put(valore, s);
			}
			else if (((String) s).contains("2008/2009")){
				Integer valore = (Integer) finalMap.getKey(s)*(-1);
				map08_09.put(valore, s);
			}
			else if (((String) s).contains("2009/2010")){
				Integer valore = (Integer) finalMap.getKey(s)*(-1);
				map09_10.put(valore, s);
			}
			else if (((String) s).contains("2010/2011")){
				Integer valore = (Integer) finalMap.getKey(s)*(-1);
				map10_11.put(valore, s);
			}
			else if (((String) s).contains("2011/2012")){
				Integer valore = (Integer) finalMap.getKey(s)*(-1);
				map11_12.put(valore, s);
			}
		}

		List<SortedBidiMap> mapList = new LinkedList<SortedBidiMap>();

		mapList.add(map06_07);
		mapList.add(map07_08);
		mapList.add(map08_09);
		mapList.add(map09_10);
		mapList.add(map10_11);
		mapList.add(map11_12);

		//creo una mappa per ogni quarto
		// <valore, 1st nome anno>
		// <valore, 1st nome anno>
		// <valore, 1st nome anno>

		SortedBidiMap map1st = new DualTreeBidiMap();
		SortedBidiMap map2nd = new DualTreeBidiMap();
		SortedBidiMap map3rd = new DualTreeBidiMap();
		SortedBidiMap map4th = new DualTreeBidiMap();
		SortedBidiMap mapOverTime = new DualTreeBidiMap();

		for (SortedBidiMap map : mapList) {
			for (Object value : map.values()) {
				String valueString = (String) value;
				String[] valueColumn = valueString.split(" ");
				if (valueColumn[0].equals("1ST"))
					map1st.put(map.getKey(value), value);
				else if (valueColumn[0].equals("2ND"))
					map2nd.put(map.getKey(value), value);
				else if (valueColumn[0].equals("3RD"))
					map3rd.put(map.getKey(value), value);
				else if (valueColumn[0].equals("4TH"))
					map4th.put(map.getKey(value), value);
				else if (valueColumn[0].equals("OverTime"))
					mapOverTime.put(map.getKey(value), value);
			}
		}

		List<String> listaQuasiFinale = new LinkedList<String>();
		for (Object o : map1st.values()) {
			Integer value = (Integer) map1st.getKey(o)*(-1);
			listaQuasiFinale.add((String) o + "\t" + value);
		}
		for (Object o : map2nd.values()) {
			Integer value = (Integer) map2nd.getKey(o)*(-1);
			listaQuasiFinale.add((String) o + "\t" + value);
		}
		for (Object o : map3rd.values()) {
			Integer value = (Integer) map3rd.getKey(o)*(-1);
			listaQuasiFinale.add((String) o + "\t" + value);
		}
		for (Object o : map4th.values()) {
			Integer value = (Integer) map4th.getKey(o)*(-1);
			listaQuasiFinale.add((String) o + "\t" + value);
		}
		for (Object o : mapOverTime.values()) {
			Integer value = (Integer) mapOverTime.getKey(o)*(-1);
			listaQuasiFinale.add((String) o + "\t" + value);
		}

		List<String> listaFinale = new LinkedList<String>();

		int cont1st = 0;
		int cont2nd = 0;
		int cont3rd = 0;
		int cont4th = 0;
		int contOverTime = 0;

		for (String s : listaQuasiFinale) {
			if (s.contains("1ST") && cont1st < 3) {
				listaFinale.add(s);
				cont1st++;
			}
			else if (s.contains("2ND") && cont2nd < 3) {
				listaFinale.add(s);
				cont2nd++;
			}
			else if (s.contains("3RD") && cont3rd < 3) {
				listaFinale.add(s);
				cont3rd++;
			}
			else if (s.contains("4TH") && cont4th < 3) {
				listaFinale.add(s);
				cont4th++;
			}
			else if (s.contains("OverTime") && contOverTime < 3) {
				listaFinale.add(s);
				contOverTime++;
			}
		}

		for (int i = 0; i < listaFinale.size(); i++) {
			System.out.println(listaFinale.get(i));
		}

		sc.stop();

	}
}