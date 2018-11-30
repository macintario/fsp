package org.example
import org.apache.flink.api.scala._
import org.apache.flink.ml.recommendation._
import org.apache.flink.ml.common.ParameterMap


object als {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val inputDS: DataSet[(Int, Int, Double)] = env.readCsvFile[(Int,
      Int, Double)]("/home/aulae1-b6/map-reduce/fsp/src/main/resources/input.csv")
    // Setup the ALS learner
    val als = ALS()
      .setIterations(50)
      .setNumFactors(10)
      .setBlocks(100)
      .setTemporaryPath("tmp")
    // Set the other parameters via a parameter map
    val parameters = ParameterMap()
      .add(ALS.Lambda, 0.5)
      .add(ALS.Seed, 42L)
    // Calculate the factorization
    als.fit(inputDS, parameters)
    // Read the testing dataset from a csv file
    val testingDS: DataSet[(Int, Int)] = env.readCsvFile[(Int, Int)]("/home/aulae1-b6/map-reduce/fsp/src/main/resources/test-data.txt")
    // Calculate the ratings according to the matrix factorization
    val predictedRatings = als.predict(testingDS)
    predictedRatings.writeAsCsv("output").setParallelism(1)
    env.execute("Flink Recommendation App")
  }
}