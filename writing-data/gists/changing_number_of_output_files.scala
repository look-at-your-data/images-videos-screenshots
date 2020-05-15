import spark.implicits._
import java.io.File
import scala.reflect.io.Directory

def getListOfFiles(directoryPath: String): List[File] = {
    val directory = new File(directoryPath)
    if (directory.exists && directory.isDirectory) {
        directory.listFiles.filter(_.isFile).toList
    } else {
        List[File]()
    }
}

val numbers = Range(0,10000).toDF

numbers.write.mode("overwrite").save("numbers.parquet")

println {
    s"By default Spark created ${getListOfFiles("numbers.parquet").size} number of files"
}

numbers.coalesce(1).write.mode("overwrite").save("numbers.parquet")

println {
    s"After coalesce Spark created ${getListOfFiles("numbers.parquet").size} number of files"
}

numbers.repartition(100).write.mode("overwrite").save("numbers.parquet")

println {
    s"After repartitioning Spark created ${getListOfFiles("numbers.parquet").size} number of files"
}

new Directory(new File("numbers.parquet")).deleteRecursively()
