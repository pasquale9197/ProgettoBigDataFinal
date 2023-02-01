import pyspark
import time
import os
import sys
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

spark = SparkSession.builder.appName("Main").getOrCreate()

#creazione DataFrame
dfOrders = spark.read.option("header", True).csv("orders.csv")
dfOrderProductsPrior = spark.read.option("header", True).csv("order_products__prior.csv")
dfOrderProductsTrain = spark.read.option("header", True).csv("order_products__train.csv")
dfAisles = spark.read.option("header", True).csv("aisles.csv")
dfDepartments = spark.read.option("header", True).csv("departments.csv")
dfProducts = spark.read.option("header", True).csv("products.csv").persist()

#creazione view
dfOrders.createOrReplaceTempView("Orders")
dfOrderProductsPrior.createOrReplaceTempView("OrderProductsPrior")
dfOrderProductsTrain.createOrReplaceTempView("OrderProductsTrain")
dfAisles.createOrReplaceTempView("Aisles")
dfDepartments.createOrReplaceTempView("Departments")
dfProducts.createOrReplaceTempView("Products")

dfOrderUnified = spark.sql("SELECT * FROM OrderProductsPrior UNION ALL SELECT * FROM OrderProductsTrain")
dfOrderUnified.createOrReplaceTempView("OrderUnified")


#ritorna le coppie (user_id, numero di ordini effettuati da user_id)
def topClientiOrdini():

    return spark.sql("SELECT user_id, COUNT(*) FROM Orders GROUP BY user_id").rdd

#ritorna le coppie (product_name, quantità comprata di product)
def topProdottiComprati():

    return spark.sql("SELECT product_name, COUNT(*) "
              "FROM OrderUnified INNER JOIN Products ON OrderUnified.product_id = Products.product_id "
              "GROUP BY product_name").rdd

#ritorna le coppie (order_id, numero di prodotti in order_id)
def ordiniPiuProdotti():

    return spark.sql("SELECT order_id, COUNT(*) FROM OrderUnified GROUP BY order_id").rdd


#ritorna le coppie (aisle, numero di prodotti di aisle venduti)
def corridoioBestSeller():

    start = time.time()
    dfIdAisle_Quantita = spark.sql("SELECT Products.aisle_id, COUNT(*) AS quantita "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "GROUP BY Products.aisle_id")

    dfIdAisle_Quantita.createOrReplaceTempView("Aisle_Quantita")

    spark.sql("SELECT Aisles.aisle, Aisle_Quantita.quantita "
              "FROM Aisle_Quantita INNER JOIN Aisles ON Aisle_Quantita.aisle_id = Aisles.aisle_id").show()
    end = time.time()
    print(end-start)

#ritorna le coppie (order_hour_of_day, numero di ordini order_hour_of_day)
def oraBestSeller():

   return spark.sql("SELECT order_hour_of_day, COUNT(*) "
             "FROM Orders "
             "GROUP BY order_hour_of_day").rdd


#ritorna le coppie (order_dow, numero di ordini venduti dow)
def giornoBestSeller():

   return spark.sql("SELECT order_dow, COUNT(*) "
              "FROM Orders "
              "GROUP BY order_dow ORDER BY order_dow").rdd


#ritorna le quadruple (user_id, giorno, ora, numero ordini fatti da user_id in giorno e ora)
def topOraGiornoAcquistoUtente():

    return spark.sql("SELECT user_id, order_dow, order_hour_of_day, COUNT(*) "
              "FROM Orders "
              "GROUP BY user_id, order_dow, order_hour_of_day").rdd


def topProdottiRiordinatiPerGiorno():

    return spark.sql("SELECT OrderUnified.product_id, Orders.order_dow, COUNT(*) "
             "FROM Orders INNER JOIN OrderUnified ON Orders.order_id = OrderUnified.order_id "
             "GROUP BY OrderUnified.product_id, Orders.order_dow").rdd

def prodottiComuniPiuAcquistati():

    spark.sql("select OrderProductsTrain.product_id, count(OrderProductsTrain.order_id) as n "
              "from OrderProductsTrain inner join OrderProductsTrain as ou on OrderProductsTrain.order_id != ou.order_id and OrderProductsTrain.product_id = ou.product_id "
              "group by OrderProductsTrain.product_id order by n desc")

def dipStessoCorridoioDiversiProd():

    dfDepartAisle = spark.sql("SELECT Products.department_id, Products.aisle_id, COUNT(*) AS number "
            "FROM Products "
            "GROUP BY Products.department_id, Products.aisle_id")

    dfDepartAisle.createOrReplaceTempView("DepartAisle")

    spark.sql("SELECT DISTINCT Departments.department, Aisles.aisle, DepartAisle.number "
              "FROM Departments INNER JOIN DepartAisle ON Departments.department_id = DepartAisle.department_id "
              "INNER JOIN Aisles ON Aisles.aisle_id = DepartAisle.aisle_id "
              "ORDER BY DepartAisle.number DESC").show(100)


def prodottiAisle(aisle):

    prodottiAisle = spark.sql("SELECT Products.product_name, Aisles.aisle, Products.department_id "
                              "FROM Products INNER JOIN Aisles ON Aisles.aisle_id = Products.aisle_id "
                              "WHERE Aisles.aisle = %a" % aisle)

    prodottiAisle.createOrReplaceTempView("ProdottiAisles")

    spark.sql("SELECT ProdottiAisles.product_name, ProdottiAisles.aisle, Departments.department "
              "FROM ProdottiAisles INNER JOIN Departments ON Departments.department_id = ProdottiAisles.department_id ").show(100)

def prodottiDepartment(department):

    prodottiDep = spark.sql("SELECT Products.product_name, Departments.department, Products.aisle_id "
                              "FROM Products INNER JOIN Departments ON Departments.department_id = Products.department_id "
                              "WHERE Departments.department = %a" % department)

    prodottiDep.createOrReplaceTempView("ProdottiDep")

    spark.sql("SELECT ProdottiDep.product_name, ProdottiDep.department, Aisles.aisle "
              "FROM ProdottiDep INNER JOIN Aisles ON Aisles.aisle_id = ProdottiDep.aisle_id ").show(100)

def prodottiAisleDep(aisle, department):

    productAisels = spark.sql("SELECT Products.product_id, Products.product_name, Aisles.aisle, Products.department_id "
              "FROM Products INNER JOIN Aisles ON Products.aisle_id = Aisles.aisle_id "
              "WHERE Aisles.aisle = %a" % aisle)
    productAisels.createOrReplaceTempView("ProductAisles")

    spark.sql("SELECT ProductAisles.product_name, ProductAisles.product_id, ProductAisles.aisle, Departments.department "
              "FROM ProductAisles INNER JOIN Departments ON ProductAisles.department_id = Departments.department_id "
              "WHERE Departments.department = %a" % department).show(50)


def ordiniUtente(id_utente):

    spark.sql("SELECT user_id, COUNT(*) "
              "FROM Orders "
              "WHERE user_id = %a "
              "GROUP BY user_id" % id_utente).show(50)


def ordiniUtenteGiorno(id_utente, giorno):

    spark.sql("SELECT user_id, COUNT(*) "
        "FROM Orders "
        "WHERE user_id = {} AND order_dow = {} "
        "GROUP BY user_id".format(id_utente, giorno)).show()


def ordiniUtenteOra(id_utente, ora):

    spark.sql("SELECT user_id, COUNT(*) "
        "FROM Orders "
        "WHERE user_id = {} AND order_hour_of_day = {} "
        "GROUP BY user_id ".format(id_utente, ora)).show()

def utenteUltimoOrdine(id_utente):
    ordine = spark.sql("SELECT Orders.user_id, OrderProductsTrain.order_id, OrderProductsTrain.product_id, "
                       "OrderProductsTrain.add_to_cart_order, OrderProductsTrain.reordered "
                       "FROM Orders INNER JOIN OrderProductsTrain ON Orders.order_id = OrderProductsTrain.order_id "
                       "WHERE Orders.user_id = %a" % id_utente)
    ordine.createOrReplaceTempView("Ordine")

    spark.sql("SELECT Ordine.order_id, Products.product_name, Ordine.add_to_cart_order, Ordine.reordered "
              "FROM Ordine INNER JOIN Products ON Products.product_id = Ordine.product_id").show(100)

def daysSincePriorOrderUtente(id_user):

    spark.sql("SELECT DISTINCT Orders.days_since_prior_order "
              "FROM Orders INNER JOIN OrderProductsTrain ON Orders.order_id = OrderProductsTrain.order_id "
              "WHERE Orders.user_id = %a" % id_user).show(1000)


def prodottoAcquistato(id_product):

    spark.sql("SELECT Products.product_name, COUNT(*) "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "WHERE Products.product_id = %a "
              "GROUP BY Products.product_name" % id_product).show()

def prodottoRiordinato(id_product):

    spark.sql("SELECT Products.product_name, COUNT(*) "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "WHERE Products.product_id = %a AND OrderUnified.reordered = 1 "
              "GROUP BY Products.product_name" % id_product).show()


def prodottiComuni(product_name):

    prodottiCorrelati = spark.sql("SELECT OrderUnified.order_id, Products.product_id, Products.product_name  "
            "FROM Products INNER JOIN OrderUnified ON Products.product_id = OrderUnified.product_id "
            "WHERE Products.product_name = %a" % product_name).cache()
    prodottiCorrelati.createOrReplaceTempView("ProdottiCorrelati")

    idProdotti = spark.sql("SELECT OrderUnified.product_id, COUNT(*) AS n "
              "FROM ProdottiCorrelati INNER JOIN OrderUnified ON ProdottiCorrelati.order_id = OrderUnified.order_id "
              "WHERE OrderUnified.product_id != ProdottiCorrelati.product_id "
              "GROUP BY OrderUnified.product_id ORDER BY n DESC LIMIT 5")
    idProdotti.createOrReplaceTempView("idProdotti")

    spark.sql("SELECT Products.product_name, idProdotti.n "
              "FROM idProdotti INNER JOIN Products ON Products.product_id = idProdotti.product_id").show(10)


def posizione(posizione):

    posizione = spark.sql("SELECT product_id, COUNT(*) AS n "
              "FROM OrderUnified "
              "WHERE add_to_cart_order = %a "
              "GROUP BY product_id" % posizione)
    posizione.createOrReplaceTempView("Posizione")

    spark.sql("SELECT Products.product_name, Posizione.n "
              "FROM Posizione INNER JOIN Products ON Products.product_id = Posizione.product_id"
              " ORDER BY n DESC").show()

def posizionePrioritaria():

    spark.sql("SELECT DISTINCT TOP 1 product_id, add_to_cart_order, COUNT(product_id) AS n "
              "FROM OrderUnified "
              "GROUP BY add_to_cart_order, product_id ORDER BY add_to_cart_order ASC, n DESC ").show()


def AislesDepartmentsRiacquistati():
    prodottiRiordinati = spark.sql("SELECT product_id "
                                   "FROM OrderUnified "
                                   "WHERE reordered = 1")
    prodottiRiordinati.createOrReplaceTempView("ProdottiRiordinati")

    spark.sql("SELECT Products.aisle_id, Products.department_id, COUNT(ProdottiRiordinati.product_id) AS n "
                "FROM Products INNER JOIN ProdottiRiordinati ON ProdottiRiordinati.product_id = Products.product_id "
                "GROUP BY Products.aisle_id, Products.department_id ORDER BY n DESC LIMIT 10").show()

def numeroOrdine(user_id, order_id):

    spark.sql("SELECT order_number "
              "FROM Orders "
              "WHERE order_id = {} AND user_id =  {}".format(order_id, user_id)).show()

corridoioBestSeller()