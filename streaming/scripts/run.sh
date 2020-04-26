$SPARK_HOME/bin/spark-submit \
  --master "local[8]" \
  --class "com.twilio.learn.PredictionStream" \
  target/spark-redis-predict.jar \
  conf/app.yaml