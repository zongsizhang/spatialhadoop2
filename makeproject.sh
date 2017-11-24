mvn assembly:assembly -DskipTests
cd target/
cp -f spatialhadoop-2.4.3-SNAPSHOT.jar spatialhadoop-2.4.2.jar
scp spatialhadoop-2.4.2.jar hduser@en4119509l:/hdd2/code/hadoop-2.6.5/share/hadoop/common/lib/
cd ..
