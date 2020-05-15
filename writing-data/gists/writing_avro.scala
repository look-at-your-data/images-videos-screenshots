// Will only work if you have the corresponding avro jar in class path
import spark.implicits._
import org.apache.spark.sql.SaveMode._
spark.range(1,1000).write.mode(Overwrite).format("avro").save("numbers.avro")
