javac -d bin -cp libraries/protobuf-java-3.1.0.jar:. src/phaseI/{Client,Hdfs,RemoteInterfaces,DataNodeRemoteInterfaces}.java
cd bin
java -Djava.rmi.server.codebase=file:$PWD/ phaseI.Client
