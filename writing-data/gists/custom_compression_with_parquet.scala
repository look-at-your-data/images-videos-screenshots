import spark.implicits._

case class Customer(id : Int, name : String)

val customersDF = Seq(
    Customer(1,"Dhruv"),
    Customer(2,"Atif"),
    Customer(3,"Nisha")
).toDF

customersDF.write.option("compression","none").mode("overwrite").save("nocompression.parquet")
customersDF.write.option("compression","gzip").mode("overwrite").save("gzipcompression.parquet")
