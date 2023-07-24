package com.wimbee

import org.apache.spark.SparkContext

object BankingMain extends App {
  val sc = new SparkContext("local", "Banking")
  private val ds = BankDataAnalysis.loadDataFromCSV(sc, "bank_data.csv")
  BankDataAnalysis.handleMissingData(ds)
  BankDataAnalysis.calculateBasicStatistics(ds)
  BankDataAnalysis.customerTransactionFrequency(ds).foreach(println)
  private val transactions = BankDataAnalysis.groupByTransactionDate(ds, "daily")
  transactions.foreach(println)
  BankDataAnalysis.plotTransactionTrends(ds)
 // val segments = BankDataAnalysis.customerSegmentation(ds)
//  BankDataAnalysis.calculateAvgTransactionAmount(ds, segments ).foreach(println)
  private val patterns = BankDataAnalysis.identifyTransactionPatterns(ds)
 // BankDataAnalysis.visualizeTransactionPatterns(patterns)
}
