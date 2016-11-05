javac -d bin -cp libraries/protobuf-java-3.1.0.jar:. src/phaseI/{DataNode,Hdfs,RemoteInterfaces,DataNodeRemoteInterfaces}.java
cd bin
java phaseI.DataNode
