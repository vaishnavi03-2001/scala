package com.wipro.dev.big_data_insights.scala.products.com.wipro.dev.big_data_insights.scala.products

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, functions}

object driver extends App {

  // creating a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()
//part1

  // schema for products table
  val mydf = StructType(Array(
    StructField("productID", IntegerType, false),

    StructField("productCode", StringType, false),

    StructField("name", StringType, false),
    StructField("quantity", IntegerType, false),

    StructField("price", FloatType, false),
    StructField("supplierid", IntegerType, false)
  ))
  //read csv file from directory path using schema
  val df = spark.read
    .format("com.databricks.spark.csv")
    .schema(mydf) // enforce a schema
    .option("delimiter", "\t").option("header", "true").option("inferSchema", "false").load("/Users/JO20382899/Downloads/csv/insight_data/products.csv")

  // Register the DataFrame as a global temporary view
  df.createGlobalTempView("products")

//Select and show all the records with quantity >= 5000 and name starts with 'Pen'
  spark.sql("SELECT * FROM global_temp.products WHERE quantity >= 5000 AND name LIKE 'Pen%' ").show()

 // Select and show all the records with quantity >= 5000, price is less than 1.24 and name starts with 'Pen'
  spark.sql("SELECT * FROM global_temp.products WHERE quantity >= 5000 AND price < 1.24 AND name LIKE 'Pen%' ").show()

  //Select and show all the records witch does not have quantity 5000 and name does not starts with 'Pen'
  spark.sql("SELECT * FROM global_temp.products WHERE NOT (quantity >= 5000 AND name LIKE 'Pen%') ").show()

 // Select and show all the products which name is 'Pen Red', 'Pen Black'
  spark.sql("SELECT * FROM global_temp.products WHERE name IN ('Pen Red','Pen Black') ").show()

//Select and show all the products which has price BETWEEN 1.0 AND 2 0 AND quantity
  spark.sql("SELECT * FROM global_temp.products WHERE price BETWEEN 1.0 AND 2.0  AND quantity = 2000").show()

  //Select all the products which has product code as null
  spark.sql("SELECT * FROM global_temp.products WHERE productCode IS NULL").show()

 //Select all the products, whose name stalls with Pen and results should be order by Price descending order.
  spark.sql("SELECT * FROM global_temp.products WHERE name LIKE '%Pen%' ORDER BY price DESC ").show()

  //Select all the products, whose name staffs with Pen and results should be order by Price descending order and quantity ascending order.
  spark.sql("SELECT * FROM global_temp.products WHERE name LIKE 'Pen%' ORDER BY price DESC, quantity ASC ").show()

  //Select top 2 products by price
  spark.sql("SELECT * FROM global_temp.products ORDER BY price DESC LIMIT 2").show()

  //Select all the columns from product table with output header as below. productID AS ID code AS Code name AS Description price AS 'Unit Price'
  spark.sql("SELECT productID AS ID , productCode AS Code , name AS Description , price AS UnitPrice FROM global_temp.products").show()

  //Select code and name both separated by '-' and header name should be ProductDescription'_
  spark.sql("SELECT CONCAT(productID,'-',name) AS ProductDescription FROM global_temp.products").show()

  //Select all distinct prices.
  spark.sql("SELECT DISTINCT price FROM  global_temp.products").show()

  //Select distinct price and name combination
  spark.sql("SELECT DISTINCT price , name FROM  global_temp.products").show()

  //Select all price data sorted by both code and productID combinatiom
  spark.sql("SELECT  price FROM  global_temp.products ORDER BY productID, productCode ASC").show()

  //count number of products.
  spark.sql("SELECT COUNT(*)  FROM  global_temp.products").show()

  //Count number of products for each code
  spark.sql("SELECT COUNT(*),productCode  FROM  global_temp.products GROUP BY productCode").show()

  //Select Maximum, minimum, average Standard Deviation, and total quantity _
  spark.sql("SELECT MAX(quantity), MIN(quantity), AVG(quantity), STDDEV(quantity), COUNT(quantity) FROM  global_temp.products").show()

  //Select minimum and maximum price for each product code.
    spark.sql("SELECT MAX(price), MIN(price) FROM  global_temp.products GROUP BY productCode").show()

  //Select Maximum, minimum, average Standard Deviation, and total quantity for each product code make sure and Standard deviation will have maximum two decimal values.
  spark.sql("SELECT MAX(quantity), MIN(quantity), AVG(quantity), STDDEV(quantity), COUNT(quantity) FROM  global_temp.products GROUP BY productCode").show()

  //Select all the product code and average price only where product count is more than or equal to 3
  spark.sql("SELECT AVG(price),AVG(productCode)  FROM  global_temp.products HAVING COUNT(*) >= 3").show()


  //part 2


// schema for supplier table
  val mdf = StructType(Array(

    StructField("supplierID", StringType, false),
    StructField("name", StringType, false),
    StructField("phone", LongType, false)
  ))

  //read csv file from directory path using schema
  val vdf = spark.read
    .format("com.databricks.spark.csv")
    .schema(mdf)
    .option("delimiter", ",")
    .option("header", "true"). option("multiline", "true").load("/Users/JO20382899/Downloads/csv/insight_data/supplier.csv")
  vdf.createGlobalTempView("supplier")
  vdf.show()

//schema for products_supplier table
  val mvdf = StructType(Array(
    StructField("productID", StringType, false),
    StructField("supplierID", IntegerType, false)
  ))

  //read csv file from directory path using schema
  val cvdf = spark.read
    .format("com.databricks.spark.csv")
    .schema(mvdf) // enforce a schema
    .option("delimiter", ",")
  .option("header", "true").option("multiline", "true").load("/Users/JO20382899/Downloads/csv/insight_data/products_supplier.csv")
  cvdf.createGlobalTempView("products_suppliers")
  cvdf.show()

  //Select product, its price , its supplier name where product price is less than 0.6 using SparkSQL
  spark.sql("SELECT global_temp.products.name,price, global_temp.supplier.name AS sup_name  FROM global_temp.products JOIN global_temp.supplier ON global_temp.products.supplierid = global_temp.supplier.supplierID WHERE price < 0.6 ").show()

  //Find all the supllier name, who are supplying 'Pencil 3B'
spark.sql("SELECT supplierID FROM global_temp.products WHERE name = 'Pencil 3B' ").show()

  //Find all the products , which are supplied by ABC Traders
  spark.sql("SELECT global_temp.products.name FROM global_temp.products INNER JOIN global_temp.supplier ON global_temp.products.supplierid = global_temp.supplier.supplierID WHERE global_temp.supplier.name = 'ABC Traders' ").show()

  //It is possible that, same product can be supplied by multiple supplier. Now find each product, its price according to each supplier.
  spark.sql("SELECT global_temp.products.name , global_temp.supplier.name AS sup_name, price FROM global_temp.products INNER JOIN global_temp.supplier ON global_temp.products.supplierid = global_temp.supplier.supplierID INNER JOIN  global_temp.products_suppliers ON global_temp.products.supplierid = global_temp.products_suppliers.supplierID  ").show()


}