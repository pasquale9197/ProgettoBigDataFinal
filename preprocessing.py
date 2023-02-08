import pyspark
import os
import sys
from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Main").config("spark.driver.memory", "15g").getOrCreate()
#creazione DataFrame
dfOrdersDB = spark.read.option("header", True).csv("orders.csv")
dfOrderProductsPrior = spark.read.option("header", True).csv("order_products__prior.csv")
dfOrderProductsTrain = spark.read.option("header", True).csv("order_products__train.csv")
dfAisles = spark.read.option("header", True).csv("aisles.csv")
dfDepartments = spark.read.option("header", True).csv("departments.csv")
dfProducts = spark.read.option("header", True).csv("products.csv")

#creazione view
dfOrderProductsPrior.createOrReplaceTempView("OrderProductsPrior")
dfOrderProductsTrain.createOrReplaceTempView("OrderProductsTrain")
dfAisles.createOrReplaceTempView("Aisles")
dfDepartments.createOrReplaceTempView("Departments")
dfProducts.createOrReplaceTempView("Products")


dfOrderUnified = spark.sql("SELECT * FROM OrderProductsPrior UNION ALL SELECT * FROM OrderProductsTrain")
dfOrderUnified.createOrReplaceTempView("OrderUnified")

dfOrdersDB.createOrReplaceTempView("OrdersDB")
dfOrders = spark.sql("SELECT * FROM OrdersDB WHERE eval_set != 'test'")
dfOrders.createOrReplaceTempView("Orders")

#preprocessing
'''
Controlla se esistono prodotti in OrderUnified che non esistono in Products (dovuti magari ad errori di battitura)
'''
def prodottiInesistentiInOrder():

    r = spark.sql("SELECT OrderUnified.product_id "
                   "FROM OrderUnified "
                   "WHERE OrderUnified.product_id NOT IN ( SELECT Products.product_id FROM Products)").count()
    print("Prodotti presenti in qualche ordine ma non presenti in Products")

def controlloChiaviAisles():

    spark.sql("SELECT Aisles.aisle_id, COUNT(Aisles.aisle_id) AS number "
              "FROM Aisles "
              "GROUP BY Aisles.aisle_id "
              "ORDER BY number DESC").show(1000)

def senzaDuplicatiAisles():

    r = spark.sql("SELECT DISTINCT Aisles.aisle_id, Aisles.aisle "
              "FROM Aisles").count()
    print("Aisles senza duplicati " + str(r))

def conDuplicatiAisles():

    r = spark.sql("SELECT Aisles.aisle_id, Aisles.aisle "
              "FROM Aisles").count()
    print("Aisles con duplicati " + str(r))

def controlloChiaviDepartments():

    spark.sql("SELECT Departments.department_id, COUNT(Departments.department_id) AS number "
              "FROM Departments "
              "GROUP BY Departments.department_id "
              "ORDER BY number DESC").show(1000)

def senzaDuplicatiDepartments():

    r = spark.sql("SELECT DISTINCT Departments.department_id, Departments.department "
              "FROM Departments").count()
    print("Departments senza duplicati " + str(r))

def conDuplicatiDepartments():

    r = spark.sql("SELECT Departments.department_id, Departments.department "
              "FROM Departments").count()
    print("Departments con duplicati " + str(r))

def controlloChiaviProducts():

    spark.sql("SELECT Products.product_id, COUNT(Products.product_id) AS number "
              "FROM Products "
              "GROUP BY Products.product_id "
              "ORDER BY number DESC").show(1000)

def senzaDuplicatiProducts():

    r = spark.sql("SELECT DISTINCT Products.product_id, Products.product_name, Products.aisle_id, Products.department_id "
              "FROM Products").count()
    print("Products senza duplicati " + str(r))

def conDuplicatiProducts():

    r = spark.sql("SELECT Products.product_id, Products.product_name, Products.aisle_id, Products.department_id "
              "FROM Products").count()
    print("Products con duplicati " + str(r))

def controlloChiaviOrders():

    spark.sql("SELECT Orders.order_id, COUNT(Orders.order_id) AS number "
              "FROM Orders "
              "GROUP BY Orders.order_id "
              "ORDER BY number DESC").show(1000)

def senzaDuplicatiOrders():

    r = spark.sql("SELECT DISTINCT Orders.order_id, Orders.user_id, Orders.eval_set, Orders.order_number, Orders.order_dow, Orders.order_hour_of_day, Orders.days_since_prior_order "
              "FROM Orders").count()
    print("Orders senza duplicati " + str(r))

def conDuplicatiOrders():

    r = spark.sql("SELECT Orders.order_id, Orders.user_id, Orders.eval_set, Orders.order_number, Orders.order_dow, Orders.order_hour_of_day, Orders.days_since_prior_order "
              "FROM Orders").count()
    print("Orders con duplicati " + str(r))

def controlloChiaviOrderProductsPrior():

    spark.sql("SELECT OrderProductsPrior.order_id, OrderProductsPrior.product_id, OrderProductsPrior.add_to_cart_order, COUNT(*) AS number "
              "FROM OrderProductsPrior "
              "GROUP BY OrderProductsPrior.order_id, OrderProductsPrior.product_id, OrderProductsPrior.add_to_cart_order "
              "ORDER BY number DESC").show(1000)

def senzaDuplicatiOrderProductsPrior():

    r = spark.sql("SELECT DISTINCT OrderProductsPrior.order_id, OrderProductsPrior.product_id, OrderProductsPrior.add_to_cart_order, OrderProductsPrior.reordered "
              "FROM OrderProductsPrior").count()
    print("OrderProductsPrior senza duplicati " + str(r))

def conDuplicatiOrderProductsPrior():

    r = spark.sql("SELECT OrderProductsPrior.order_id, OrderProductsPrior.product_id, OrderProductsPrior.add_to_cart_order, OrderProductsPrior.reordered "
              "FROM OrderProductsPrior").count()
    print("OrderProductsPrior con duplicati " + str(r))

def controlloChiaviOrderProductsTrain():

    spark.sql("SELECT OrderProductsTrain.order_id, OrderProductsTrain.product_id, OrderProductsTrain.add_to_cart_order, COUNT(*) AS number "
              "FROM OrderProductsTrain "
              "GROUP BY OrderProductsTrain.order_id, OrderProductsTrain.product_id, OrderProductsTrain.add_to_cart_order "
              "ORDER BY number DESC").show(1000)

def senzaDuplicatiOrderProductsTrain():

    r = spark.sql("SELECT DISTINCT OrderProductsTrain.order_id, OrderProductsTrain.product_id, OrderProductsTrain.add_to_cart_order, OrderProductsTrain.reordered "
              "FROM OrderProductsTrain").count()
    print("OrderProductsTrain senza duplicati " + str(r))

def conDuplicatiOrderProductsTrain():

    r = spark.sql("SELECT OrderProductsTrain.order_id, OrderProductsTrain.product_id, OrderProductsTrain.add_to_cart_order, OrderProductsTrain.reordered "
              "FROM OrderProductsTrain").count()
    print("OrderProductsTrain con duplicati " + str(r))

prodottiInesistentiInOrder()

'''
senzaDuplicatiAisles()
conDuplicatiAisles()
senzaDuplicatiDepartments()
conDuplicatiDepartments()
senzaDuplicatiProducts()
conDuplicatiProducts()
senzaDuplicatiOrders()
conDuplicatiOrders()
senzaDuplicatiOrderProductsPrior()
conDuplicatiOrderProductsPrior()
senzaDuplicatiOrderProductsTrain()
conDuplicatiOrderProductsTrain()


controlloChiaviAisles()
controlloChiaviDepartments()
controlloChiaviProducts()
controlloChiaviOrders()
controlloChiaviOrderProductsPrior()
controlloChiaviOrderProductsTrain()
'''