package NBA_Project.NBA_Project;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.bson.BSONObject;

import com.mongodb.hadoop.MongoInputFormat;
public class AppTest {
	public static void main(String[] args) {
		String logFile = "C:/Users/Giuseppe/Desktop/basketProjectBD/basketProjectBD/datasetNBA/2006_2007 PO.txt";
		SparkConf conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> logData = sc.textFile(logFile).cache();

		//Setting mongodb https://github.com/mongodb/mongo-hadoop/wiki/Spark-Usage
		Configuration mongodbConfig = new Configuration();
		mongodbConfig.set("mongo.job.input.format", "com.mongodb.hadoop.MongoInputFormat");
		mongodbConfig.set("mongo.input.uri", "mongodb://localhost:27017/NBA.2006_2007_PO");
		JavaPairRDD<Object, BSONObject> documents = sc.newAPIHadoopRDD(
				mongodbConfig,            // Configuration
				MongoInputFormat.class,   // InputFormat: read from a live cluster.
				Object.class,             // Key class
				BSONObject.class          // Value class
				);
		System.out.println("ASD " + documents.toString());
		long numAs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("a"); }
		}).count();
		long numBs = logData.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { return s.contains("b"); }
		}).count();
		System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	}
}