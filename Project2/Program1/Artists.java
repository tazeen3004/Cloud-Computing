package artists;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class Artists {
    public static void total_count(String filename )
    {
        SparkConf conf = new SparkConf().setMaster("yarn").setAppName("total_count");

        SparkSession part = SparkSession.builder().config(conf).getOrCreate();

        Dataset<Row> artist = part.read().format("com.databricks.spark.csv").option("delimiter", "\t").option("inferSchema", "true")
                .option("header", "true").load("hdfs:///user/root/accesslog/user_artists.dat");

        Dataset<Row>  colu = artist.groupBy(artist.col("artistID")).sum("weight");
        Dataset<Row> total = colu.sort(colu.col("sum(weight)").desc());
        total.show(Integer.MAX_VALUE ,false);

    }

    public static void main( String[] args )
    {
        total_count( "" );
    }

}
