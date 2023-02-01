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
dfProducts = spark.read.option("header", True).csv("products.csv")

#creazione view
dfOrders.createOrReplaceTempView("Orders")
dfOrderProductsPrior.createOrReplaceTempView("OrderProductsPrior")
dfOrderProductsTrain.createOrReplaceTempView("OrderProductsTrain")
dfAisles.createOrReplaceTempView("Aisles")
dfDepartments.createOrReplaceTempView("Departments")
dfProducts.createOrReplaceTempView("Products")

dfOrderUnified = spark.sql("SELECT * FROM OrderProductsPrior UNION ALL SELECT * FROM OrderProductsTrain")
dfOrderUnified.createOrReplaceTempView("OrderUnified")

'''
Clienti che hanno effettuato più ordini
:return user_id, numero di ordini effettuati da user_id)
*** FAST LOADING ***
'''
def topClientiOrdini():

    return spark.sql("SELECT user_id, COUNT(*) FROM Orders GROUP BY user_id").rdd

'''
Prodotti più acquistati
:return product_name, quantità comprata di product
*** MEDIUM LOADING ***
'''
def topProdottiComprati():

    return spark.sql("SELECT product_name, COUNT(*) "
              "FROM OrderUnified INNER JOIN Products ON OrderUnified.product_id = Products.product_id "
              "GROUP BY product_name").rdd


'''
Restituisce gli ordini con più prodotti
:return order_id, numero di prodotti in order_id
*** FAST LOADING ***
'''
def ordiniPiuProdotti():

     return spark.sql("SELECT order_id, COUNT(*) FROM OrderUnified GROUP BY order_id")

'''
Restituisce il corridoio che ha venduto più prodotti
:return aisle, numero di prodotti di aisle venduti
# *** MEDIUM LOADING ***
'''
def corridoioBestSeller():

    dfIdAisle_Quantita = spark.sql("SELECT Products.aisle_id, COUNT(*) AS quantita "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "GROUP BY Products.aisle_id")

    dfIdAisle_Quantita.createOrReplaceTempView("Aisle_Quantita")

    return spark.sql("SELECT Aisles.aisle, Aisle_Quantita.quantita "
              "FROM Aisle_Quantita INNER JOIN Aisles ON Aisle_Quantita.aisle_id = Aisles.aisle_id").rdd

'''
Ritorna l'ora in cui si vende di più
:return order_hour_of_day, numero di ordini order_hour_of_day
*** FAST LOADING ***
'''
def oraBestSeller():

   return spark.sql("SELECT order_hour_of_day, COUNT(*) "
             "FROM Orders "
             "GROUP BY order_hour_of_day").rdd

'''
Ritorna il giorno in cui si vende di più
:return order_dow, numero di ordini venduti dow
*** FAST LOADING ***
'''
def giornoBestSeller():

   return spark.sql("SELECT order_dow, COUNT(*) "
              "FROM Orders "
              "GROUP BY order_dow ORDER BY order_dow").rdd

'''
Ritorna l'utente che ha comprato di più per ogni giorno e per ogni ora
:return user_id, giorno, ora, numero ordini effettuati
# *** FAST LOADING ***
'''
def topOraGiornoAcquistoUtente():

    return spark.sql("SELECT user_id, order_dow, order_hour_of_day, COUNT(*) "
              "FROM Orders "
              "GROUP BY user_id, order_dow, order_hour_of_day").rdd

'''
Ritorna i prodotti che sono stati più riordinati per ogni giorno della settimana
:return product_id, order_dow, #prodotti
*** SO SLOW LOADING *** 
'''
#
def topProdottiRiordinatiPerGiorno():

    return spark.sql("SELECT OrderUnified.product_id, Orders.order_dow, COUNT(*) "
             "FROM Orders INNER JOIN OrderUnified ON Orders.order_id = OrderUnified.order_id "
             "GROUP BY OrderUnified.product_id, Orders.order_dow").rdd


'''
Restituisce i prodotti acquistati più comunemente
:return product_id, #order_id
*** FAST LOADING ***
'''
def prodottiComuniPiuAcquistati():

    spark.sql("select OrderProductsTrain.product_id, count(OrderProductsTrain.order_id) as n "
              "from OrderProductsTrain inner join OrderProductsTrain as ou on OrderProductsTrain.order_id != ou.order_id and OrderProductsTrain.product_id = ou.product_id "
              "group by OrderProductsTrain.product_id order by n desc")

'''
Ritorna il numero di prodotti diversi di uno stesso dipartimento all'interno di uno stesso corridoio
:return department_name, aisle_name, #products
*** FAST LOADING ***
'''
def dipStessoCorridoioDiversiProd():

    dfDepartAisle = spark.sql("SELECT Products.department_id, Products.aisle_id, COUNT(*) AS number "
            "FROM Products "
            "GROUP BY Products.department_id, Products.aisle_id")

    dfDepartAisle.createOrReplaceTempView("DepartAisle")

    spark.sql("SELECT DISTINCT Departments.department, Aisles.aisle, DepartAisle.number "
              "FROM Departments INNER JOIN DepartAisle ON Departments.department_id = DepartAisle.department_id "
              "INNER JOIN Aisles ON Aisles.aisle_id = DepartAisle.aisle_id "
              "ORDER BY DepartAisle.number DESC")


'''
Restituisce tutti i prodotti presenti in uno specifico corridoio
:param aisle
:return product_name, aisle_name, department_name
*** FAST LOADING ***
'''
def prodottiAisle(aisle):

    prodottiAisle = spark.sql("SELECT Products.product_name, Aisles.aisle, Products.department_id "
                              "FROM Products INNER JOIN Aisles ON Aisles.aisle_id = Products.aisle_id "
                              "WHERE Aisles.aisle = %a" % aisle)
    prodottiAisle.createOrReplaceTempView("ProdottiAisles")

    spark.sql("SELECT ProdottiAisles.product_name, ProdottiAisles.aisle, Departments.department "
              "FROM ProdottiAisles INNER JOIN Departments ON Departments.department_id = ProdottiAisles.department_id ")


'''
Ritorna tutti i prodotti appartenenti ad uno specifico dipartimento
:param department_name
:return product_name, department_name, aisle_name
*** FAST LOADING ***
'''
def prodottiDepartment(department):

    prodottiDep = spark.sql("SELECT Products.product_name, Departments.department, Products.aisle_id "
                              "FROM Products INNER JOIN Departments ON Departments.department_id = Products.department_id "
                              "WHERE Departments.department = %a" % department)
    prodottiDep.createOrReplaceTempView("ProdottiDep")

    spark.sql("SELECT ProdottiDep.product_name, ProdottiDep.department, Aisles.aisle "
              "FROM ProdottiDep INNER JOIN Aisles ON Aisles.aisle_id = ProdottiDep.aisle_id ")

'''
Ritorna tutti i prodotti appartenenti ad uno specifico corridoio e dipartimento
:param aisle_name
:param department_name
:return product_name, product_id, department_name, aisle_name
*** FAST LOADING ***
'''
def prodottiAisleDep(aisle, department):

    productAisels = spark.sql("SELECT Products.product_id, Products.product_name, Aisles.aisle, Products.department_id "
              "FROM Products INNER JOIN Aisles ON Products.aisle_id = Aisles.aisle_id "
              "WHERE Aisles.aisle = %a" % aisle)
    productAisels.createOrReplaceTempView("ProductAisles")

    spark.sql("SELECT ProductAisles.product_name, ProductAisles.product_id, ProductAisles.aisle, Departments.department "
              "FROM ProductAisles INNER JOIN Departments ON ProductAisles.department_id = Departments.department_id "
              "WHERE Departments.department = %a" % department)

'''
Restituisce tutti gli ordini di un utente specifico
:param id_utente
:return user_id, #ordini
*** FAST LOADING ***
'''
def ordiniUtente(id_utente):

    spark.sql("SELECT user_id, COUNT(*) "
              "FROM Orders "
              "WHERE user_id = %a "
              "GROUP BY user_id" % id_utente)

'''
Restituisce tutti gli ordini di uno specifico utente in uno specifico giorno
:param id_utente
:param giorno
:return user_id, #ordini
*** FAST LOADING ***
'''
def ordiniUtenteGiorno(id_utente, giorno):

    spark.sql("SELECT user_id, COUNT(*) "
        "FROM Orders "
        "WHERE user_id = {} AND order_dow = {} "
        "GROUP BY user_id".format(id_utente, giorno))


'''
Restituisce tutti gli ordini di uno specifico utente in una specifica ora
:param id_utente
:param ora
:return user_id, #ordini
*** FAST LOADING ***
'''
def ordiniUtenteOra(id_utente, ora):

    spark.sql("SELECT user_id, COUNT(*) "
        "FROM Orders "
        "WHERE user_id = {} AND order_hour_of_day = {} "
        "GROUP BY user_id ".format(id_utente, ora))


'''
Restituisce l'ultimo ordine di uno specifico utente
:param id_utente
:return order_id, product_name, add_to_cart_order, reordered
*** FAST LOADING ***
'''
def utenteUltimoOrdine(id_utente):
    ordine = spark.sql("SELECT Orders.user_id, OrderProductsTrain.order_id, OrderProductsTrain.product_id, "
                       "OrderProductsTrain.add_to_cart_order, OrderProductsTrain.reordered "
                       "FROM Orders INNER JOIN OrderProductsTrain ON Orders.order_id = OrderProductsTrain.order_id "
                       "WHERE Orders.user_id = %a" % id_utente)
    ordine.createOrReplaceTempView("Ordine")

    spark.sql("SELECT Ordine.order_id, Products.product_name, Ordine.add_to_cart_order, Ordine.reordered "
              "FROM Ordine INNER JOIN Products ON Products.product_id = Ordine.product_id")


'''
Restituisce il tempo intercorso tra l'ultimo ordine di uno specifico utente e quello precedente da lui effettuato
:param id_user
:return days_since_prior_order
*** FAST LOADING ***
'''
def daysSincePriorOrderUtente(id_user):

    spark.sql("SELECT DISTINCT Orders.days_since_prior_order "
              "FROM Orders INNER JOIN OrderProductsTrain ON Orders.order_id = OrderProductsTrain.order_id "
              "WHERE Orders.user_id = %a" % id_user)

'''
Ritorna il numero di volte che il prodotto specifico è stato acquistato
:param id_product
:return product_name, #ordini
*** MEDIUM LOADING ***
'''
def prodottoAcquistato(id_product):

    spark.sql("SELECT Products.product_name, COUNT(*) "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "WHERE Products.product_id = %a "
              "GROUP BY Products.product_name" % id_product)

'''
Restituisce il numero di volte che lo specifico prodotto è stato riordinato
:param id_product
:return product_name, #ordini
*** MEDIUM LOADING ***
'''
def prodottoRiordinato(id_product):

    spark.sql("SELECT Products.product_name, COUNT(*) "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "WHERE Products.product_id = %a AND OrderUnified.reordered = 1 "
              "GROUP BY Products.product_name" % id_product)

'''
Analisi Data Mining, restituisce gli elementi correlati ad un altro acquistato. Nello specifico, dato un prodotto
cerca quali sono i prodotti più acquistati da tutti gli utenti che hanno acquistato lo specifico prodotto.
Cerca i prodotti correlati tra di loro. Il metodo sceglie la top 5 dei prodotti correlati.
:param product_name
:return product_name, #prodotti
*** SO SLOW LOADING ***
'''
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
              "FROM idProdotti INNER JOIN Products ON Products.product_id = idProdotti.product_id")


'''
Data una posizione, restituisce tutti i prodotti in quella posizione in tutti gli ordini
:param posizione
:return product_name, #ordini
*** MEDIUM LOADING ***
'''
def posizione(posizione):

    posizione = spark.sql("SELECT product_id, COUNT(*) AS n "
              "FROM OrderUnified "
              "WHERE add_to_cart_order = %a "
              "GROUP BY product_id" % posizione)
    posizione.createOrReplaceTempView("Posizione")

    spark.sql("SELECT Products.product_name, Posizione.n "
              "FROM Posizione INNER JOIN Products ON Products.product_id = Posizione.product_id"
              " ORDER BY n DESC")


'''
Restituisce quali sono gli alimenti con più priorità acquistati, ovvero quelli che sono stati aggiunti
per primi nel carrello (prima posizione)
:return product_id, add_to_cart_order
*** FAST LOADING ***
'''
def posizionePrioritaria():

    spark.sql("SELECT DISTINCT product_id, add_to_cart_order, COUNT(product_id) AS n "
              "FROM OrderUnified "
              "GROUP BY add_to_cart_order, product_id ORDER BY add_to_cart_order ASC, n DESC ")


'''
Restituisce i corridoi e i dipartimenti in cui sono stati riacquistati prodotti
:return aisle_id, department_id, #prodotti
*** FAST LOADING ***
'''
def aislesDepartmentsRiacquistati():
    prodottiRiordinati = spark.sql("SELECT product_id "
                                   "FROM OrderUnified "
                                   "WHERE reordered = 1")
    prodottiRiordinati.createOrReplaceTempView("ProdottiRiordinati")

    spark.sql("SELECT Products.aisle_id, Products.department_id, COUNT(ProdottiRiordinati.product_id) AS n "
                "FROM Products INNER JOIN ProdottiRiordinati ON ProdottiRiordinati.product_id = Products.product_id "
                "GROUP BY Products.aisle_id, Products.department_id ORDER BY n DESC LIMIT 10")


'''
Dato uno specifico user e uno specifico ordine restituisce il numero di quell'ordine
:param user_id
:param order_id
:return order_number
*** FAST LOADING ***
'''
def numeroOrdine(user_id, order_id):

    spark.sql("SELECT order_number "
              "FROM Orders "
              "WHERE order_id = {} AND user_id =  {}".format(order_id, user_id))
