package ma.aitbouna;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.count;

public class SparkStream {
    public static void main(String[] args) throws TimeoutException, StreamingQueryException {
// Créer une session Spark
        SparkSession spark = SparkSession.builder()
                .appName("StreamingDataProcessing")
                .master("local[2]") // Utilisez "local[n]" pour n threads locaux
                .getOrCreate();
        spark.sparkContext().setLogLevel("ERROR");

        StructType schema = new StructType()
                .add("id", "int")
                .add("description", "string")
                .add("no_avion", "string")
                .add("date", "date");

        // Lire les données CSV en streaming à partir du répertoire HDFS
        Dataset<Row> incidents = spark.readStream()
                .option("header", true)
                .schema(schema)
                .csv("hdfs://localhost:9000/incidents/incident.csv");


        // Tâche 1 : Afficher l'avion ayant le plus d'incidents en continu
        Dataset<Row> planeWithMostIncidents = incidents.groupBy("no_avion")
                .agg(count("*").as("total_incidents"))
                .orderBy(desc("total_incidents"))
                .limit(1);

        // Définir une requête de sortie pour afficher les résultats en continu
        StreamingQuery query1 = planeWithMostIncidents.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Tâche2 : Afficher les deux mois de l'année en cours avec le moins d'incidents en continu
        Dataset<Row> incidentsByMonth = incidents.withColumn("month", month(col("date")))
                .withColumn("year", year(col("date")))
                .filter(col("year").equalTo(year(current_date())))
                .filter(col("month").equalTo(month(current_date())))
                .groupBy("year", "month")
                .agg(count("*").as("total_incidents"))
                .orderBy("total_incidents")
                .limit(2);

        // Définir une requête de sortie pour afficher les résultats en continu
        StreamingQuery query = incidentsByMonth.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        // Attendre la fin de l'exécution
        query1.awaitTermination();
        query.awaitTermination();
    }
}