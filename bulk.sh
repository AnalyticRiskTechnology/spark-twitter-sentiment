spark-submit --master yarn-client \
		--class com.dhruv.BulkPredict \
               --driver-memory 1g \
               --executor-memory 1g \
               target/twittersentiment-0.0.1-jar-with-dependencies.jar \
		trainedModel \
               hdfs:///tmp/tweets/bankdata2.csv 
