###
 # @Descripttion: 
 # @version: 
 # @Author: Heng Tyrion Wang
 # @Date: 2022-02-26 08:20:10
 # @LastEditors: Heng Tyrion Wang
 # @Email: hengtyrionwang@gmail.com
 # @LastEditTime: 2022-02-28 12:53:44
### 

path=/user/data

spark-submit --master yarn --executor-cores 4 --executor-memory 2g --num-executors 1 --driver-memory 1g --class com.example.data.tfrecord.FFMTfrecord --jars ../target/ffm-tfrecord-1.0-SNAPSHOT.jar ../libs/spark-tensorflow-connector_2.12-1.15.0.jar $path
