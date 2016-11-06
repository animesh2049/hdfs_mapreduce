javac -d bin -cp .:libraries/mysql-connector-java-5.1.40-bin.jar:libraries/protobuf-java-3.1.0.jar src/phaseI/{DataNode,Hdfs,RemoteInterfaces,DataNodeRemoteInterfaces,NameNode}.java
cd bin
java -cp .:../libraries/mysql-connector-java-5.1.40-bin.jar:../libraries/protobuf-java-3.1.0.jar phaseI.DataNode
