package com.lansrod.hive;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;

public class HiveCreate {

	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf().setAppName("Myapp")
				.setMaster("local");
		
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		HiveContext hiveContext = new HiveContext(sparkContext);
		hiveContext.setConf("hive.metastore.warehouse.dir","/home/ahmed/Bureau/test");
//		hiveContext.setConf("hive.metastore.uris", "thrift://hdpmaster2:9083");
//		 create new table if not exists
		hiveContext.sql("CREATE DATABASE IF NOT EXISTS testhivedb");
//		hiveContext.sql("CREATE EXTERNAL  TABLE IF NOT EXISTS testhivedb.hivetest (HORODATE TIMESTAMP, VA_PDC DOUBLE ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" ESCAPED BY ',' STORED AS TEXTFILE LOCATION '/home/ahmed/Bureau/hiveprojects/hivetest'");
//		hiveContext.sql("CREATE EXTERNAL TABLE IF NOT EXISTS  testhivedb.hivetest2 (HORODATE TIMESTAMP, VA_PDC DOUBLE ) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" ESCAPED BY ',' STORED AS TEXTFILE LOCATION '/home/ahmed/Bureau/hiveprojects/hivetest2'");

		hiveContext.sql("CREATE EXTERNAL  TABLE IF NOT EXISTS testhivedb.employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" ESCAPED BY ',' STORED AS TEXTFILE LOCATION '/home/ahmed/Bureau/A/B'");
		
//		hiveContext.sql("CREATE TABLE IF NOT EXISTS testhivedb.employee(id INT, name STRING, age INT) ROW FORMAT DELIMITED FIELDS TERMINATED BY \",\" ESCAPED BY ',' STORED AS TEXTFILE LOCATION '/home/ahmed/Bureau/hiveprojects/hivetest'");
//
		hiveContext.sql("insert into table testhivedb.employee select t.* from (select 1210, 'rahul', 55) t");
		hiveContext.sql("insert into table testhivedb.employee select t.* from (select 1211, 'sriram pv', 35) t");
		hiveContext.sql("insert into table testhivedb.employee select t.* from (select 1212, 'gowri', 59) t");

		
//		 select, filter and insert data
		DataFrame result = hiveContext.sql("SELECT * from testhivedb.employee");
		result.repartition(1).write().mode(SaveMode.Overwrite).saveAsTable("testhivedb.testsaveastable");
		result.show();
//
//		DataFrame resultSoutirage = hiveContext.sql("SELECT DH_MSR_PDC AS HORODATE , VA_MSR_PDC AS VA_PDC FROM opd.e_msr_poin_cour_rte WHERE trim(CD_GRAN_METR_PDC)='REF'");
//		result.write().mode(SaveMode.Overwrite).saveAsTable("opd.Soutirage_RTE");

	}

}
