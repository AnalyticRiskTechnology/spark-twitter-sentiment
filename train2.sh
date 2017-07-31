spark-submit --master yarn-client \
		--class com.dhruv.Train \
               --driver-memory 1g \
               --executor-memory 1g \
               target/twittersentiment-0.0.1-jar-with-dependencies.jar \
               hdfs:///tmp/tweets/dataset.csv \
               trainedModel
