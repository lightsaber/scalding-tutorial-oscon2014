java -Dscala.usejavacp=true -cp lib/scalding_tutorial.jar scala.tools.nsc.Main -d target Hello.scala
if [ $? = 0 ]; then
  scala -cp lib/scalding_tutorial.jar:target com.twitter.scalding.Tool Hello --local #--hdfs --tool-graph
fi
