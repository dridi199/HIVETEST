package com.lansrod.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

public class HiveDataFrame {
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Myapp").setMaster("local");
		JavaSparkContext javaContext = new JavaSparkContext(sparkConf);
		HiveContext hiveContext = new HiveContext(javaContext);
		hiveContext.setConf("hive.metastore.warehouse.dir","/home/ahmed/Bureau/test");
//		hiveContext.sql("CREATE DATABASE IF NOT EXISTS mydb");
		hiveContext.sql("USE mydb");
		hiveContext.sql("DROP table  table_temporaire");
		hiveContext.sql("CREATE EXTERNAL  TABLE IF NOT EXISTS table_temporaire(name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \";\" STORED AS TEXTFILE LOCATION '/home/ahmed/Bureau/A/B/' tblproperties (\"skip.header.line.count\"=\"1\") ");
// skip header (c'est a dire ne pas prendre en compte le header dans les fichier
//		hiveContext.sql("load data local inpath '/home/ahmed/Bureau/test.txt' into table table_temporaire");
//		hiveContext.sql("insert into table mytable_orc select * from table_temporaire"); 
		DataFrame results=hiveContext.sql("SELECT * from table_temporaire");
		results.show();
	}
}
