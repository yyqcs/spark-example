### 本项目是使用scala语言给出了spark2.4.5计算框架中各模块的常用实例。
> 温馨提醒：spark的版本与scala的版本号有严格的对应关系，安装请注意。

![spark框架图](http://spark.apache.org/images/spark-stack.png)

#### Spark Core
- [RDD以及Pair RDD的常用算子](src/main/java/rdd/RDDCommonUsage.scala)
### Spark SQL
- [DataFrame常用示例](src/main/java/dataframe/basic/DataFrameBasic.scala)
- [RDD转换为DataFrame](src/main/java/dataframe/mysql2df/Mysql2DataFrame.scala)
- [DataFrame与MySQL的交互](src/main/java/dataframe/mysql2df/Mysql2DataFrame.scala)
### Spark MLlib
- [流水线pipeline的基本用法](src/main/java/ml/basic/MLCommonUsage.scala)
- [决策树](src/main/java/ml/decisiontree/DecisionTreeUsage.scala)
- [K均值 K-means](src/main/java/ml/kmeans/KMeansUsage.scala)
- [逻辑回归 LogisticRegression](src/main/java/ml/lr/LogisticRegressionUsage.scala)
- [超参优化 网格搜索](src/main/java/ml/tuning/LRTuning.scala)
### Spark Streaming 
- [从本地文件流中数据  ssc.textFileStream](src/main/java/streaming/FileStream.scala)
- [从socket中获取数据 ssc.socketTextStream](src/main/java/streaming/SocketSoruceUsage.scala)
- [从Kafka中获取数据 KafkaUtils.createDirectStream](src/main/java/streaming/KafkaWordConsumer.scala)
- [Kafka生产数据](src/main/java/streaming/KafkaWordProducer.scala)

### Spark GraphX
- [顶点与边的创建与操作](src/main/java/graphx/GraphxBasic.scala)
- [顶点重要性度量 PageRank](src/main/java/graphx/PageRankUsage.scala)
- [图的连通分量 connectedComponents](src/main/java/graphx/ConnectedComponentsUsage.scala)
- [三角结构的数量 triangleCount](src/main/java/graphx/TriangleCountUsage.scala)

### 所用数据集
代码中用到的数据集都可以在spark安装目录的data子目录中找到，具体为`xxx\spark-2.4.5-bin-hadoop2.7\data`
### 更多的用例
完整的用例在spark安装目录的examples子目录中。
具体为：`xxx\spark-2.4.5-bin-hadoop2.7\examples\src\main\scala\org\apache\spark\examples`

### Spark的视频学习资料
推荐林子雨老师的慕课视频，质量高且免费，观看地址请[点击这儿](https://www.icourse163.org/course/XMU-1205811805)
。课程的PPT请[点击这儿](https://dblab.xmu.edu.cn/post/11073/)。

### spark书籍(电子版、可直接下载)
- [spark快速大数据分析](http://js.xiazaicc.com/down1/sparkksdsjfx_downcc.zip)
- Spark GraphX 实战:[中文](https://blog.csdn.net/qq_29447481/article/details/90212611);[英文](https://github.com/zhuxiuwei/GraphXInAction/blob/master/Spark%20GraphX%20In%20Action.pdf)

### 如果你觉得本项目对你有帮助，麻烦star支持一下。有任何问题，请新建issue交流。