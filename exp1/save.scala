import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import java.util.Calendar
import java.text.SimpleDateFormat

val data = sc.textFile("hdfs:///user/ssp573/daily_adjusted_UAL.csv")
val data_trends= sc.textFile("hdfs:///user/ssp573/file_All.csv")
val data_crudeoil = sc.textFile("hdfs:///user/ssp573/crude_oil_n.csv")

val header = data.first
val header_trends= data_trends.first
val header_crudeoil= data_crudeoil.first

val rows = data.filter(l=>l!=header)
val rows_trends = data_trends.filter(l=>l!=header_trends)
val rows_crudeoil = data_crudeoil.filter(l=>l!=header_crudeoil)

case class CC1(TimeStamp: String, Open: Double, High: Double, Low: Double, Close: Double, Adjusted_Close: Double,Volume: Long)
case class CC2(Date: String, Price_2: Double, Open_2: Double, High_2: Double, Low_2: Double, Change_Percent: Double)

val allSplit = rows.map(line=>line.split(","))
val allSplit_trends=rows_trends.map(line=>line.split(","))
val allSplit_crudeoil=rows_crudeoil.map(line=>line.split(","))

val allData = allSplit.map(p=> CC1(p(0).toString,p(1).trim.toDouble,p(2).trim.toDouble,p(3).trim.toDouble,p(4).trim.toDouble,p(5).trim.toDouble,p(6).trim.toLong))
val allData_trends= allSplit_trends.map(p=> (p(0).toString,p(1).trim.toInt,p(2).trim.toInt,p(3).trim.toInt,p(4).trim.toInt,p(5).trim.toInt,p(6).trim.toInt,p(7).trim.toInt,p(8).trim.toInt,p(9).trim.toInt,p(10).trim.toInt,p(11).trim.toInt,p(12).trim.toInt,p(13).trim.toInt,p(14).trim.toInt,p(15).trim.toInt,p(16).trim.toInt,p(17).trim.toInt,p(18).trim.toInt))
allData_trends.take(10)
val allData_crudeoil= allSplit_crudeoil.map(p=> CC2(p(0).toString,p(1).trim.toDouble,p(2).trim.toDouble,p(3).trim.toDouble,p(4).trim.toDouble,p(5).toDouble))
allData_crudeoil.take(10)

val allDF = allData.toDF()
val allDF_trends= allData_trends.toDF()
val allDF_crudeoil= allData_crudeoil.toDF()
allDF_crudeoil.show(10)

val df_jomo = allDF.as("dfjomo")
val df_panse = allDF_trends.as("dfpanse")
val df_shah = allDF_crudeoil.as("dfshah")

val join1_df = df_jomo.join(df_panse, col("dfjomo.TimeStamp") === col("dfpanse._1"),"inner")
val df_jn = join1_df.as("df_jn")
val joined_df = df_jn.join(df_shah, col("df_jn.TimeStamp") === col("dfshah.Date"),"inner")

val df1 = joined_df.select(allDF("Close").as("label"),$"Open",$"High",$"Low",$"Adjusted_Close",$"Volume",$"_2",$"_3",$"_4",$"_5",$"_6",$"_7",$"_8",$"_9",$"_10",$"_11",$"_12",$"_13",$"_14",$"_15",$"_16",$"_17",$"Price_2",$"Open_2",$"High_2",$"Low_2",$"Change_Percent")
joined_df.write.parquet("hdfs:///user/ssp573/Reg_All_Data.parquet")

