name := "log-archive-extractor"

version := "0.1"

scalaVersion := "2.12.6"


// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk-s3
libraryDependencies += "com.amazonaws" % "aws-java-sdk-s3" % "1.11.353"
// https://mvnrepository.com/artifact/org.scalaz/scalaz-core
libraryDependencies += "org.scalaz" %% "scalaz-core" % "7.2.24"
// https://mvnrepository.com/artifact/org.scalaz/scalaz-effect
libraryDependencies += "org.scalaz" %% "scalaz-effect" % "7.2.24"
// https://mvnrepository.com/artifact/com.typesafe.play/play-json
libraryDependencies += "com.typesafe.play" %% "play-json" % "2.6.9"
