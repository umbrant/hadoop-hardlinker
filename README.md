Compile with:

  mvn clean package

Run with:

  mvn exec:java -Dexec.mainClass=com.cloudera.Hardlinker -Dexec.args="generate /tmp/out1 10000"
  mvn exec:java -Dexec.mainClass=com.cloudera.Hardlinker -Dexec.args="link /tmp/out1 /tmp/out2"

Substitute your own paths as appropriate.
