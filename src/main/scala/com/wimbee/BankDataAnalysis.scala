package com.wimbee

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession, functions}
import org.jfree.chart.plot.{PlotOrientation, XYPlot}
import org.jfree.chart.{ChartFactory, ChartPanel}
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.data.xy.DefaultXYDataset

import javax.swing.JFrame

case class Transaction(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)
case class CustomerTransactionFrequency(customer_id: Int, transaction_frequency: Long)
case class TransactionPattern(customer_id: Int, transaction_date: String, transaction_type: String, amount: Double)

object BankDataAnalysis {

  def loadDataFromCSV(sc: SparkContext, filename: String): RDD[Transaction] = {
    val dataRDD = sc.textFile(filename)
    dataRDD.map(line => {
      val fields = line.split(",")
      Transaction(fields(0).toInt, fields(1), fields(2), fields(3).toDouble)
    })
  }

  def handleMissingData(data: RDD[Transaction]): RDD[Transaction] = {
    data.filter(transaction => transaction.amount > 0
      && transaction.transaction_type != null
      && transaction.transaction_date !=null
      && transaction.customer_id != 0)
  }

  def calculateBasicStatistics(data: RDD[Transaction]): Unit = {
    val totalTransactions = data.map(_.amount).sum()
    val totalDeposits = data.filter(_.transaction_type == "deposit").map(_.amount).sum()
    val percentageOfTotalDeposits = (totalDeposits / totalTransactions * 100).toInt

    val totalWithdrawals = data.filter(_.transaction_type == "withdrawal").map(_.amount).sum()
    val percentageOfTotalWithdrawals =  (totalWithdrawals / totalDeposits * 100).toInt

    val averageTransactionAmount = data.map(_.amount).mean()

    println(s"Total Deposits: $totalDeposits")
    println(s"Total Deposits Compared To Total Transactions: $percentageOfTotalDeposits%" )
    println(s"Total Withdrawals: $totalWithdrawals")
    println(s"Total Withdrawals Compared To Total Transactions: $percentageOfTotalWithdrawals%")
    println(s"Average Transaction Amount: $averageTransactionAmount")
  }

  def customerTransactionFrequency(data: RDD[Transaction]): RDD[CustomerTransactionFrequency] = {
    val transactionCounts = data.map(transaction => (transaction.customer_id, 1))
      .reduceByKey(_ + _)

    transactionCounts.map { case (customer_id, frequency) =>
      CustomerTransactionFrequency(customer_id, frequency)
    }
  }

  def groupByTransactionDate(data: RDD[Transaction], timeUnit: String): RDD[(String, Double)] = {
    val dateAmountRDD = data.map(transaction => (transaction.transaction_date, transaction.amount))
      .reduceByKey(_ + _)

    val aggregatedRDD = timeUnit.toLowerCase match {
      case "daily" => dateAmountRDD.reduceByKey(_ + _)
      case "monthly" => dateAmountRDD.map { case (date, amount) => (date.substring(0, 7), amount) }.reduceByKey(_ + _)
      case _ => throw new IllegalArgumentException("Invalid timeUnit. Supported values: daily, monthly")
    }
    aggregatedRDD
  }

  def plotTransactionTrends(data: RDD[Transaction]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[1]").appName("SparkByExamples.com")
      .getOrCreate()
    val schema = StructType(Seq(
      StructField("customer_id", IntegerType, nullable = false),
      StructField("transaction_date", StringType, nullable = false),
      StructField("transaction_type", StringType, nullable = false),
      StructField("amount", DoubleType, nullable = false),
    ))
    val df = spark.createDataFrame(data.map(Row.fromTuple), schema).toDF()
    val depositsAndWithdrawalsByDate = getDepositsAndWithdrawalsByDate(df)
    showDepositsAndWithdrawalsByDate(depositsAndWithdrawalsByDate)
  }


  private def getDepositsAndWithdrawalsByDate(data: DataFrame): DataFrame = {
    val depositsAndWithdrawals = data
      .groupBy("transaction_date")
      .agg(
        functions.sum(when(col("transaction_type") === "deposit", col("amount")).otherwise(0.0)).alias("total_deposit"),
        functions.sum(when(col("transaction_type") === "withdrawal", col("amount")).otherwise(0.0)).alias("total_withdrawal")
      )
      .orderBy("transaction_date")

    depositsAndWithdrawals.show()
    depositsAndWithdrawals
  }

  private def showDepositsAndWithdrawalsByDate(data: DataFrame): Unit = {
    val dataset = new DefaultCategoryDataset()
    data.collect().foreach { row =>
      val date = row.getAs[String]("transaction_date")
      val deposit = row.getAs[Double]("total_deposit")
      val withdrawal = row.getAs[Double]("total_withdrawal")
      dataset.addValue(deposit, "Deposits", date)
      dataset.addValue(withdrawal, "Withdrawals", date)
    }
    val chart = ChartFactory.createLineChart(
      "Trends of Deposits and Withdrawals Over Time",
      "Date",
      "Amount",
      dataset,
      PlotOrientation.VERTICAL,
      true,
      true,
      false
    )

    val frame = new JFrame("Transaction Patterns")
    frame.add(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
  }

  def customerSegmentation(data: RDD[Transaction]): RDD[(Int, String)] = {
    val customerTotalAmountRDD = data
      .map(transaction => (transaction.customer_id, transaction.amount))
      .reduceByKey(_ + _)
    val customerTransactionCountRDD = data
      .map(transaction => (transaction.customer_id, 1))
      .reduceByKey(_ + _)

    val lastTransactionDateRDD = data
      .map(transaction => (transaction.customer_id, transaction.transaction_date))
      .reduceByKey((date1, date2) => if (date1 > date2) date1 else date2)

    val joinedRDD = customerTotalAmountRDD
      .join(customerTransactionCountRDD)
      .join(lastTransactionDateRDD)

    joinedRDD.map { case (customer_id, ((totalAmount, transactionCount), lastTransactionDate)) =>
      val segmentLabel =
        if (totalAmount >= 1000) "high-value customer"
        else if (transactionCount >= 10) "frequent transactor"
        else "inactive customer"

      (customer_id, segmentLabel)
    }

  }

  def calculateAvgTransactionAmount(data: RDD[Transaction], segments: RDD[(Int, String)]): RDD[(String, Double)] = {
    val customerAmounts = data.map(transaction => (transaction.customer_id, transaction.amount))
    val joinedData = customerAmounts.join(segments)
    val avgTransactionAmounts = joinedData.map { case (_, (amount, segment)) => (segment, amount) }
      .aggregateByKey((0.0, 0L))(
        (acc, value) => (acc._1 + value, acc._2 + 1),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      .mapValues { case (totalAmount, count) => totalAmount / count.toDouble }

    avgTransactionAmounts
  }

  def identifyTransactionPatterns(data: RDD[Transaction]): RDD[TransactionPattern] = {

    val transactionsByCustomer = data.groupBy(_.customer_id)

    val sortedTransactionsByCustomer = transactionsByCustomer.mapValues(_.toList.sortBy(_.transaction_date))

    val transactionPatterns = sortedTransactionsByCustomer.flatMap {
      case (_, transactions) =>
        transactions.sliding(2).flatMap {
          case List(prevTransaction, currentTransaction) =>
            if ((prevTransaction.transaction_type == "withdrawal" && prevTransaction.amount > 500.0) &&
              (currentTransaction.transaction_type == "deposit" && currentTransaction.amount > 500.0) || (prevTransaction.transaction_type == "deposit" && prevTransaction.amount > 500.0) &&
              (currentTransaction.transaction_type == "withdrawal" && currentTransaction.amount > 500.0)) {
              Some(TransactionPattern(currentTransaction.customer_id, currentTransaction.transaction_date, currentTransaction.transaction_type, currentTransaction.amount))
            } else if(prevTransaction.amount == currentTransaction.amount){
              Some(TransactionPattern(currentTransaction.customer_id, currentTransaction.transaction_date, currentTransaction.transaction_type, currentTransaction.amount))
            }
            else {
              None
            }
          case _ => None
        }
      case _ => None
    }

    transactionPatterns

  }

  def visualizeTransactionPatterns(data: RDD[TransactionPattern]): Unit = {
    val patternsArray = data.collect()
    val dataset = new DefaultXYDataset()
    val series = patternsArray.groupBy(_.transaction_date).mapValues(_.map(_.amount).toArray).toSeq.sortBy(_._1)
    series.foreach { case (date, amounts) =>
      val data = Array(Array.range(0, amounts.length).map(_.toDouble), amounts)
      dataset.addSeries(date, data)
    }
    val chart = ChartFactory.createScatterPlot("Identified Transaction Patterns", "Transaction Date", "Transaction Amount", dataset, PlotOrientation.VERTICAL, true, true, false)
    val plot = chart.getPlot.asInstanceOf[XYPlot]
    plot.setDomainPannable(true)
    plot.setRangePannable(true)

    val frame = new JFrame("Transaction Patterns")
    frame.add(new ChartPanel(chart))
    frame.pack()
    frame.setVisible(true)
  }

}