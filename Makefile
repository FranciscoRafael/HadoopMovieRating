run:
	/usr/local/bin/hadoop com.sun.tools.javac.Main WordCountFirst.java
	jar cf wc1.jar WordCountFirst*.class
	/usr/local/bin/hdfs dfs -rm -r /user/alexandremestre/MovieTestFirstOutput
	/usr/local/bin/hdfs dfs -rm -r /user/alexandremestre/MovieTestFinalOutput
	/usr/local/bin/hadoop jar wc1.jar WordCountFirst /user/alexandremestre/testMovie /user/alexandremestre/MovieTestFirstOutput
	/usr/local/bin/hadoop com.sun.tools.javac.Main WordCountSec.java
	jar cf wc2.jar WordCountSec*.class
	/usr/local/bin/hadoop jar wc2.jar WordCountSec /user/alexandremestre/MovieTestFirstOutput /user/alexandremestre/MovieTestFinalOutput