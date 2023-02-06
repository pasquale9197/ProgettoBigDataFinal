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


#preprocessing
'''
Controlla se esistono prodotti in OrderUnified che non esistono in Products (dovuti magari ad errori di battitura)
'''
def preProcessingControlloProdotti():

    return spark.sql("SELECT OrderUnified.product_id "
                   "FROM OrderUnified "
                   "WHERE OrderUnified.product_id NOT IN ( SELECT Products.product_id FROM Products)")


dfOrderUnified = spark.sql("SELECT * FROM OrderProductsPrior UNION ALL SELECT * FROM OrderProductsTrain")
dfOrderUnified.createOrReplaceTempView("OrderUnified")

dfOrdersDB.createOrReplaceTempView("OrdersDB")
dfOrders = spark.sql("SELECT * FROM OrdersDB WHERE eval_set != 'test'")
dfOrders.createOrReplaceTempView("Orders")
dfOrders.createOrReplaceTempView("Orders")


'''
Clienti che hanno effettuato più ordini
:return user_id, numero di ordini effettuati da user_id)
*** FAST LOADING ***
'''
def topClientiOrdini():

    return spark.sql("SELECT user_id, COUNT(*) AS n FROM Orders GROUP BY user_id ORDER BY n DESC").rdd

'''
Prodotti più acquistati
:return product_name, quantità comprata di product
*** MEDIUM LOADING ***
'''
def topProdottiComprati():

    return spark.sql("SELECT product_name, COUNT(*) AS n "
              "FROM OrderUnified INNER JOIN Products ON OrderUnified.product_id = Products.product_id "
              "GROUP BY product_name "
              "ORDER BY n DESC").rdd


'''
Restituisce gli ordini con più prodotti
:return order_id, numero di prodotti in order_id
*** FAST LOADING ***
'''
def ordiniPiuProdotti():

     return spark.sql("SELECT order_id, COUNT(*) AS n FROM OrderUnified GROUP BY order_id ORDER BY n DESC").rdd

'''
Restituisce il corridoio che ha venduto più prodotti
:return aisle, numero di prodotti di aisle venduti
# *** MEDIUM LOADING ***
'''
def venditePerCorridoio():

    dfIdAisle_Quantita = spark.sql("SELECT Products.aisle_id, COUNT(*) AS quantita "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "GROUP BY Products.aisle_id ORDER BY quantita DESC")

    dfIdAisle_Quantita.createOrReplaceTempView("Aisle_Quantita")

    return spark.sql("SELECT Aisles.aisle, Aisle_Quantita.quantita "
              "FROM Aisle_Quantita INNER JOIN Aisles ON Aisle_Quantita.aisle_id = Aisles.aisle_id").rdd

'''
Ritorna l'ora in cui si vende di più
:return order_hour_of_day, numero di ordini order_hour_of_day
*** FAST LOADING ***
'''
def ordiniPerOra():

   return spark.sql("SELECT order_hour_of_day, COUNT(*) AS n " 
             "FROM Orders "
             "GROUP BY order_hour_of_day").rdd

'''
Ritorna il giorno in cui si vende di più
:return order_dow, numero di ordini venduti dow
*** FAST LOADING ***
'''
def ordiniPerGiorno():

   return spark.sql("SELECT order_dow, COUNT(*) AS n "
              "FROM Orders "
              "GROUP BY order_dow").rdd

'''
Ritorna l'utente che ha comprato di più per ogni giorno e per ogni ora
:return user_id, giorno, ora, numero ordini effettuati
# *** FAST LOADING ***
'''
def topOraGiornoAcquistoUtente():

    return spark.sql("SELECT user_id, order_dow, order_hour_of_day, COUNT(*) AS n "
              "FROM Orders "
              "GROUP BY user_id, order_dow, order_hour_of_day ORDER BY n DESC").rdd

'''
Ritorna i prodotti che sono stati più riordinati per ogni giorno della settimana
:return product_id, order_dow, #prodotti
*** SO SLOW LOADING *** 
'''
#
def topProdottiRiordinatiPerGiorno():

    q1 = spark.sql("SELECT OrderUnified.product_id, Orders.order_dow, COUNT(*) AS n "
             "FROM Orders INNER JOIN OrderUnified ON Orders.order_id = OrderUnified.order_id "
             "WHERE OrderUnified.reordered = 1 "
             "GROUP BY OrderUnified.product_id, Orders.order_dow")
    q1.createOrReplaceTempView("Q1")

    return spark.sql("SELECT Products.product_name, Q1.product_id, Q1.n "
                     "FROM Products INNER JOIN Q1 ON Products.products_id = Q1.product_id ORDER BY n DESC")

'''
Restituisce i prodotti acquistati più comunemente
:return product_id, #order_id
*** FAST LOADING ***
'''
def prodottiComuniPiuAcquistati():

    return spark.sql("select OrderProductsTrain.product_id, count(OrderProductsTrain.order_id) as n "
              "from OrderProductsTrain inner join OrderProductsTrain as ou on OrderProductsTrain.order_id != ou.order_id and OrderProductsTrain.product_id = ou.product_id "
              "group by OrderProductsTrain.product_id order by n desc").rdd

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

    return spark.sql("SELECT DISTINCT Departments.department, Aisles.aisle, DepartAisle.number "
              "FROM Departments INNER JOIN DepartAisle ON Departments.department_id = DepartAisle.department_id "
              "INNER JOIN Aisles ON Aisles.aisle_id = DepartAisle.aisle_id ").rdd


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

    return spark.sql("SELECT ProdottiAisles.product_name, ProdottiAisles.aisle, Departments.department "
              "FROM ProdottiAisles INNER JOIN Departments ON Departments.department_id = ProdottiAisles.department_id ").rdd


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

    return spark.sql("SELECT ProdottiDep.product_name, ProdottiDep.department, Aisles.aisle "
              "FROM ProdottiDep INNER JOIN Aisles ON Aisles.aisle_id = ProdottiDep.aisle_id ").rdd

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

    return spark.sql("SELECT ProductAisles.product_name, ProductAisles.product_id, ProductAisles.aisle, Departments.department "
              "FROM ProductAisles INNER JOIN Departments ON ProductAisles.department_id = Departments.department_id "
              "WHERE Departments.department = %a" % department).rdd

'''
Restituisce tutti gli ordini di un utente specifico
:param id_utente
:return user_id, #ordini
*** FAST LOADING ***
'''
def ordiniUtente(id_utente):

    return spark.sql("SELECT user_id, COUNT(*) AS n "
              "FROM Orders "
              "WHERE user_id = %a "
              "GROUP BY user_id ORDER BY n DESC" % id_utente).rdd

'''
Restituisce tutti gli ordini di uno specifico utente in uno specifico giorno
:param id_utente
:param giorno
:return user_id, #ordini
*** FAST LOADING ***
'''
def ordiniUtenteGiorno(id_utente, giorno):

    return spark.sql("SELECT user_id, COUNT(*) AS n "
        "FROM Orders "
        "WHERE user_id = {} AND order_dow = {} "
        "GROUP BY user_id ORDER BY n DESC".format(id_utente, giorno)).rdd


'''
Restituisce tutti gli ordini di uno specifico utente in una specifica ora
:param id_utente
:param ora
:return user_id, #ordini
*** FAST LOADING ***
'''
def ordiniUtenteOra(id_utente, ora):

    return spark.sql("SELECT user_id, COUNT(*) AS n "
        "FROM Orders "
        "WHERE user_id = {} AND order_hour_of_day = {} "
        "GROUP BY user_id ORDER BY n DESC".format(id_utente, ora)).rdd


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

    return spark.sql("SELECT Ordine.order_id, Products.product_name, Ordine.add_to_cart_order, Ordine.reordered "
              "FROM Ordine INNER JOIN Products ON Products.product_id = Ordine.product_id").rdd


'''
Restituisce il tempo intercorso tra l'ultimo ordine di uno specifico utente e quello precedente da lui effettuato
:param id_user
:return days_since_prior_order
*** FAST LOADING ***
'''
def daysSincePriorOrderUtente(id_user):

    return spark.sql("SELECT DISTINCT Orders.days_since_prior_order "
              "FROM Orders INNER JOIN OrderProductsTrain ON Orders.order_id = OrderProductsTrain.order_id "
              "WHERE Orders.user_id = %a" % id_user).rdd

'''
Ritorna il numero di volte che il prodotto specifico è stato acquistato
:param id_product
:return product_name, #ordini
*** MEDIUM LOADING ***
'''
def prodottoAcquistato(id_product):

    return spark.sql("SELECT Products.product_name, COUNT(*) AS n "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "WHERE Products.product_id = %a "
              "GROUP BY Products.product_name" % id_product).rdd

'''
Restituisce il numero di volte che lo specifico prodotto è stato riordinato
:param id_product
:return product_name, #ordini
*** MEDIUM LOADING ***
'''
def prodottoRiordinato(id_product):

    return spark.sql("SELECT Products.product_name, COUNT(*) "
              "FROM Products INNER JOIN OrderUnified ON OrderUnified.product_id = Products.product_id "
              "WHERE Products.product_id = %a AND OrderUnified.reordered = 1 "
              "GROUP BY Products.product_name" % id_product).rdd

'''
Analisi Data Mining, restituisce gli elementi correlati ad un altro acquistato. Nello specifico, dato un prodotto
cerca quali sono i prodotti più acquistati da tutti gli utenti che hanno acquistato lo specifico prodotto.
Cerca i prodotti correlati tra di loro. Il metodo sceglie la top 5 dei prodotti correlati.
:param product_name
:return product_name, #prodotti
*** SO SLOW LOADING ***
'''
def top7ProdottiCorrelati(product_name):

    ordiniProductName = spark.sql("SELECT OrderUnified.order_id, OrderUnified.product_id "
            "FROM Products INNER JOIN OrderUnified ON Products.product_id = OrderUnified.product_id "
            "WHERE Products.product_name = %a" % product_name)
    ordiniProductName.createOrReplaceTempView("OrdiniProductName")

    prodottiCorrelati = spark.sql("SELECT OrderUnified.product_id, COUNT(*) AS n "
            "FROM OrdiniProductName INNER JOIN OrderUnified ON OrdiniProductName.order_id = OrderUnified.order_id "
            "WHERE OrderUnified.product_id != OrdiniProductName.product_id "
            "GROUP BY OrderUnified.product_id ORDER BY n DESC LIMIT 7")
    prodottiCorrelati.createOrReplaceTempView("ProdottiCorrelati")

    return spark.sql("SELECT Products.product_name, ProdottiCorrelati.n "
              "FROM ProdottiCorrelati INNER JOIN Products ON Products.product_id = ProdottiCorrelati.product_id "
              "ORDER BY ProdottiCorrelati.n DESC").rdd

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

    return spark.sql("SELECT Products.product_name, Posizione.n "
              "FROM Posizione INNER JOIN Products ON Products.product_id = Posizione.product_id"
              " ORDER BY n DESC").rdd


'''
Restituisce quali sono gli alimenti con più priorità acquistati, ovvero quelli che sono stati aggiunti
per primi nel carrello (prima posizione)
:return product_id, add_to_cart_order
*** FAST LOADING ***
'''
def posizionePrioritaria():

    return spark.sql("SELECT DISTINCT product_id, add_to_cart_order, COUNT(product_id) AS n "
              "FROM OrderUnified "
              "GROUP BY add_to_cart_order, product_id ORDER BY add_to_cart_order ASC, n DESC ").rdd


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

    return spark.sql("SELECT Products.aisle_id, Products.department_id, COUNT(ProdottiRiordinati.product_id) AS n "
                "FROM Products INNER JOIN ProdottiRiordinati ON ProdottiRiordinati.product_id = Products.product_id "
                "GROUP BY Products.aisle_id, Products.department_id").rdd


'''
Dato uno specifico user e uno specifico ordine restituisce il numero di quell'ordine
:param user_id
:param order_id
:return order_number
*** FAST LOADING ***
'''
def numeroOrdine(user_id, order_id):

    return spark.sql("SELECT order_number "
              "FROM Orders "
              "WHERE order_id = {} AND user_id =  {}".format(order_id, user_id)).rdd


'''
Ritorna il prodotto più acquistato e la sua variazione di vendite durante la settimana
:return product_name, order_dow #prodottiAcquistati
*** SLOW LOADING ***
'''
def prodottoPiuAcquistato():

    mostBrought = spark.sql("SELECT product_id, COUNT(product_id) AS n "
                              "FROM OrderUnified "
                              "GROUP BY product_id ORDER BY n DESC LIMIT 1")
    mostBrought.createOrReplaceTempView("MostBrought")

    q1 = spark.sql("SELECT Orders.order_id, OrderUnified.product_id, Orders.order_dow "
                   "FROM Orders INNER JOIN OrderUnified ON Orders.order_id = OrderUnified.order_id ")
    q1.createOrReplaceTempView("q1")

    return spark.sql("SELECT Products.product_name, q1.order_dow, COUNT(MostBrought.product_id) AS n "
              "FROM MostBrought INNER JOIN q1 ON MostBrought.product_id = q1.product_id "
              "INNER JOIN Products ON MostBrought.product_id = Products.product_id "
              "GROUP BY Products.product_name, q1.order_dow ORDER BY q1.order_dow").rdd
'''
Ritorna il/i prodotto/i meno acquistato e la sua variazione di vendite durante la settimana
:return product_name, order_dow, #vendite
*** SLOW LOADING ***
'''
def prodottoMenoAcquistato():

    lessBrought = spark.sql("SELECT product_id, COUNT(product_id) AS n "
                              "FROM OrderUnified "
                              "GROUP BY product_id ORDER BY n ASC LIMIT 1")
    lessBrought.createOrReplaceTempView("lessBrought")

    q1 = spark.sql("SELECT Orders.order_id, OrderUnified.product_id, Orders.order_dow "
                   "FROM Orders INNER JOIN OrderUnified ON Orders.order_id = OrderUnified.order_id ")
    q1.createOrReplaceTempView("q1")

    return spark.sql("SELECT Products.product_name, q1.order_dow, COUNT(lessBrought.product_id) AS n "
              "FROM lessBrought INNER JOIN q1 ON lessBrought.product_id = q1.product_id "
              "INNER JOIN Products ON lessBrought.product_id = Products.product_id "
              "GROUP BY Products.product_name, q1.order_dow ORDER BY q1.order_dow").rdd

'''
Ritorna il prodotto chiesto dall'utente e la variazione di acquisti durante la settimana
:return product_name, order_dow, #vendite
*** SLOW LOADING ***
'''
def prodottoSceltoDaUtente(prodotto):
    lessBrought = spark.sql("SELECT Products.product_name, OrderUnified.product_id, COUNT(OrderUnified.product_id) AS n "
                              "FROM OrderUnified INNER JOIN Products ON Products.product_id = OrderUnified.product_id "
                              "GROUP BY Products.product_name, OrderUnified.product_id")
    lessBrought.createOrReplaceTempView("lessBrought")

    q2 = spark.sql("SELECT product_name, product_id, n "
                   "FROM lessBrought WHERE product_name = %a" %prodotto)
    q2.createOrReplaceTempView("q2")

    q1 = spark.sql("SELECT Orders.order_id, OrderUnified.product_id, Orders.order_dow "
                   "FROM Orders INNER JOIN OrderUnified ON Orders.order_id = OrderUnified.order_id ")
    q1.createOrReplaceTempView("q1")

    return spark.sql("SELECT q2.product_name, q1.order_dow, COUNT(q2.product_name) AS n "
              "FROM q2 INNER JOIN q1 ON q2.product_id = q1.product_id "
              "GROUP BY q2.product_name, q1.order_dow "
              "ORDER BY q1.order_dow").rdd

'''
Ritorna i 20 prodotti più venduti nel fine settimana
:return product_name, order_dow, #vendite
*** SLOW LOADING ***
'''
def top15ProdottiWeekend():

    q1 = spark.sql("SELECT order_id, order_dow "
                   "FROM Orders WHERE order_dow = 5 OR order_dow = 6")
    q1.createOrReplaceTempView("Q1")

    q2 = spark.sql("SELECT product_id, COUNT(product_id) AS n "
                   "FROM Q1 INNER JOIN OrderUnified ON Q1.order_id = OrderUnified.order_id "
                   "GROUP BY product_id ORDER BY n")
    q2.createOrReplaceTempView("Q2")

    return spark.sql("SELECT Products.product_name, Q2.n "
                     "FROM Q2 INNER JOIN Products ON Q2.product_id = Products.product_id "
                     "ORDER BY Q2.n DESC LIMIT 20").rdd

def prodottiSoloInTrain():

    q1 = spark.sql("SELECT OrderProductsTrain.product_id "
                   "FROM OrderProductsTrain "
                   "EXCEPT "
                   "SELECT OrderProductsPrior.product_id "
                   "FROM OrderProductsPrior")
    q1.createOrReplaceTempView("Q1")

    q2 = spark.sql("SELECT Q1.product_id, COUNT(*) AS n "
                   "FROM Q1 "
                   "GROUP BY Q1.product_id")
    q2.createOrReplaceTempView("Q2")

    return spark.sql("SELECT Products.product_name, Q2.n "
                     "FROM Products INNER JOIN Q2 ON Q2.product_id = Products.product_id "
                     "ORDER BY n DESC")

'''
Restituisce i prodotti invenduti 
:return product_id, order_id
*** MEDIUM LOADING ***
'''
def prodottiInvenduti():

    return spark.sql("SELECT Products.product_id, Products.product_name "
                   "FROM Products "
                   "WHERE product_id NOT IN ( SELECT OrderUnified.product_id FROM OrderUnified)").rdd

'''
Restituisce i 10 prodotti più acquistati da un utente
:return product_name, #prodotti
*** SLOW LOADING ***
'''
def top10ProdottiUtente(user_id):

    q1 = spark.sql("SELECT user_id, order_id "
                   "FROM Orders WHERE user_id = %a" %user_id)
    q1.createOrReplaceTempView("q1")

    return spark.sql("SELECT Products.product_name, COUNT(OrderUnified.product_id) AS n "
              "FROM q1 INNER JOIN OrderUnified ON q1.order_id = OrderUnified.order_id "
              "INNER JOIN Products ON Products.product_id = OrderUnified.product_id "
              "GROUP BY Products.product_name ORDER BY n DESC LIMIT 10").rdd

'''
Ritorna l'ora in cui un prodotto è stato acquistato più spesso
:return product_name, order_hour_of_day, #prodotti
*** SLOW LOADING ***
'''
def prodottiAcquistatiOra(product_name):

    q1 = spark.sql("SELECT OrderUnified.order_id, Products.product_id, Products.product_name "
                   "FROM OrderUnified INNER JOIN Products ON OrderUnified.product_id = Products.product_id")
    q1.createOrReplaceTempView("q1")

    q2 = spark.sql("SELECT order_id, product_id, product_name "
                   "FROM q1 WHERE product_name = %a" %product_name)
    q2.createOrReplaceTempView("q2")

    return spark.sql("SELECT q2.product_name, Orders.order_hour_of_day, COUNT(q2.product_name) AS n "
              "FROM Orders INNER JOIN q2 ON Orders.order_id = q2.order_id "
              "GROUP BY Orders.order_hour_of_day, q2.product_name ORDER BY n DESC").rdd

'''
Ritorna la variazione del numero di prodotti venduti in OrderProductsTrain durante la settimana
:return order_dow #prodottiAcquistati
*** SLOW LOADING ***
'''
def variazioneOrderTrain():

    mostBrought = spark.sql("SELECT product_id, COUNT(product_id) AS n "
                              "FROM OrderProductsTrain "
                              "GROUP BY product_id ORDER BY n DESC")
    mostBrought.createOrReplaceTempView("MostBrought")

    q1 = spark.sql("SELECT Orders.order_id, OrderProductsTrain.product_id, Orders.order_dow "
                   "FROM Orders INNER JOIN OrderProductsTrain ON Orders.order_id = OrderProductsTrain.order_id ")
    q1.createOrReplaceTempView("q1")

    return spark.sql("SELECT q1.order_dow, COUNT(MostBrought.product_id) AS n "
              "FROM MostBrought INNER JOIN q1 ON MostBrought.product_id = q1.product_id "
              "INNER JOIN Products ON MostBrought.product_id = Products.product_id "
              "GROUP BY q1.order_dow ORDER BY q1.order_dow").rdd

'''
Ritorna la variazione del numero di prodotti venduti in OrderProductsPrior durante la settimana
:return order_dow #prodottiAcquistati
*** SLOW LOADING ***
'''

def variazioneOrderPrior():

    mostBrought = spark.sql("SELECT product_id, COUNT(product_id) AS n "
                              "FROM OrderProductsPrior "
                              "GROUP BY product_id ORDER BY n DESC")
    mostBrought.createOrReplaceTempView("MostBrought")

    q1 = spark.sql("SELECT Orders.order_id, OrderProductsPrior.product_id, Orders.order_dow "
                   "FROM Orders INNER JOIN OrderProductsPrior ON Orders.order_id = OrderProductsPrior.order_id ")
    q1.createOrReplaceTempView("q1")

    spark.sql("SELECT q1.order_dow, COUNT(MostBrought.product_id) AS n "
              "FROM MostBrought INNER JOIN q1 ON MostBrought.product_id = q1.product_id "
              "INNER JOIN Products ON MostBrought.product_id = Products.product_id "
              "GROUP BY q1.order_dow ORDER BY q1.order_dow").rdd

top7ProdottiCorrelati("Pure Coconut Water With Orange")