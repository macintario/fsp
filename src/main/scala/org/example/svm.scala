package org.example
import org.apache.flink.api.scala._
import org.apache.flink.ml.math.Vector
import org.apache.flink.ml.common.LabeledVector
import org.apache.flink.ml.classification.SVM
import org.apache.flink.ml.RichExecutionEnvironment

object svm {
  def main(args: Array[String]) {
    // set up the execution environment
    val pathToTrainingFile: String = "/home/aulae1-b6/map-reduce/fsp/src/main/resources/iris.csv"
    val pathToTestingFile: String = "/home/aulae1-b6/map-reduce/fsp/src/main/resources/iris.csv"
    val env = ExecutionEnvironment.getExecutionEnvironment
// Read the training dataset, from a LibSVM formatted file
val trainingDS: DataSet[LabeledVector] =
  env.readLibSVM(pathToTrainingFile)
    // Create the SVM learner
    val svm = SVM()
      .setBlocks(10)
    // Learn the SVM model
    svm.fit(trainingDS)
    // Read the testing dataset
    val testingDS: DataSet[Vector] =
      env.readLibSVM(pathToTestingFile).map(_.vector)
    // Calculate the predictions for the testing dataset
    val predictionDS: DataSet[(Vector, Double)] =
      svm.predict(testingDS)
    predictionDS.writeAsText("/home/aulae1-b6/map-reduce/fsp/src/main/resources/out.txt")
    env.execute("Flink SVM App")
  }
}