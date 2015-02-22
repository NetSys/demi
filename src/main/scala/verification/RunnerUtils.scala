package akka.dispatch.verification

import scala.sys.process._
import scala.sys.process.BasicIO

import java.io._
import scala.io._

object Experiment {

  def create_experiment_dir(experiment_name: String) : String = {
    // Create experiment dir.
    // ("./interposition/src/main/python/setup.py -n " + name) .!
    var output_dir = ""
    val errToDevNull = BasicIO(false, (out) => output_dir = out, None)
    val proc = (f"./interposition/src/main/python/setup.py -t -n " + experiment_name).run(errToDevNull)
    // Block until the process exits.
    proc.exitValue
    return output_dir.trim
  }

  def record_experiment(experiment_name: String, trace: EventTrace, violation: ViolationFingerprint) {
    val output_dir = Experiment.create_experiment_dir(experiment_name)
    //val traceFile = new File(output_dir + "/trace.json")
    //val traceFileOut = new TextFileOutput(traceFile)
    //trace.serializeToFile(traceFileOut)
    println(trace.serialize())
    //traceFileOut.close()

    val violationFile = new File(output_dir + "/violation.json")
    //val violationFileOut = new TextFileOutput(violationFile)
    violation.serialize()
    //violationFileOut.close()
  }

  def read_experiment(results_dir: String) {
    // TODO(cs): halp!
    // val json = Source.fromFile(results_dir + "/trace.json").getLines.mkString
  }
}

object RunnerUtils {
}
