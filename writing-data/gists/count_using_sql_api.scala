import spark.implicits._

case class Customer(id : Int, name : String)

val someCustomers = Seq(
    Customer(1,"Dhruv"),
    Customer(2,"Atif"),
    Customer(3,"Nisha")
)

someCustomers.toDF.write.saveAsTable("customers")

spark.sql("select count(*) from customers").show
