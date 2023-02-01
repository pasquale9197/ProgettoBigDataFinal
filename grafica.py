import tkinter
from tkinter import *
from tkinter import ttk
import tkinter as tk
import os
import sys
import main as metodi
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure

import pyspark
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import numpy as np

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

#crea la finestra principale
root = Tk()
root.configure(background="#35363a")
root.title("InstaCart Online Analysis")

#definisce altezza, larghezza, x, y
root.geometry("700x400+50+50")

#logo
logo = tkinter.PhotoImage(file="analysis.png")
label1 = Label(root, image=logo, background="#35363a")
label1.pack(ipady=10, ipadx=50)

#defisce il frame contenente le opzioni di ricerca
frameRicerca = Frame(root, background="#3b4754", border=10)
frameRicerca.pack(side=TOP, fill=X, padx=50, pady=10)

#defisce il frame contenente i risultati (elenco e grafico)
frameRisultati = Frame(root, background="#313335", border=10)
frameRisultati.pack(expand=True, fill=BOTH, padx=50)
#frameRisultati.pack(expand=True, fill=BOTH, padx=50, side=BOTTOM)

#defisce il frame contenente l'elenco dei risultati
frameElenco = Frame(frameRisultati, background="#3c3f41")
frameElenco.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
labelElenco = Label(frameElenco, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
labelElenco.pack(ipady=10)

#defisce il frame contenente il grafico
frameGrafico = Frame(frameRisultati, background="#3c3f41")
frameGrafico.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
labelGrafico = Label(frameGrafico, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
labelGrafico.pack(ipady=10)

query_selezionata = StringVar()

def risultatoElenco():

    print("ho premuto cerca")
    lista_vuota = []
    risultato = lista_vuota
    column = []
    tabella = ttk.Treeview(frameElenco, column=("c1", "c2"), show='headings')

    match query_selezionata.get():
        case "topClientiOrdini":
            print("sto eseguendo topClientiOrdini")
            labelDescrizioneRicerca = Label(frameRicerca, text="Lo scopo di questa analisi è scoprire per ogni cliente C, quanti ordini ha effettuato C.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.topClientiOrdini()
            tabella.heading("c1", text="ID Cliente")
            tabella.heading("c2", text="Num.Ordini effettuati")
            print("topClientiOrdini - terminato")
        case "topProdottiComprati":
            print("sto eseguendo topProdottiComprati")
            labelDescrizioneRicerca = Label(frameRicerca, text="Lo scopo di questa analisi è scoprire per ogni prodotto P, qual è la quantità venduta di P.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.topProdottiComprati()
            tabella.heading("c1", text="Nome Prodotto")
            tabella.heading("c2", text="Quantità venduta")
            print("topProdottiComprati - terminato")
        case "ordiniPiuProdotti":
            print("sto eseguendo ordiniPiuProdotti")
            labelDescrizioneRicerca = Label(frameRicerca, text="Lo scopo di questa analisi è scoprire per ogni ordine O, qual è la quantità di prodotti contenuti in O.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.ordiniPiuProdotti()
            tabella.heading("c1", text="ID Ordine")
            tabella.heading("c2", text="Quantità di prodotti contenuti")
            print("ordiniPiuProdotti - terminato")
        case "corridoioBestSeller":
            print("sto eseguendo corridoioBestSeller")
            labelDescrizioneRicerca = Label(frameRicerca, text="Lo scopo di questa analisi è scoprire per ogni asle A, qual è il numero di prodotti venduti da A.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.corridoioBestSeller()
            tabella.heading("c1", text="Aisle")
            tabella.heading("c2", text="Prodotti venduti da aisle")
            print("corridoioBestSeller - terminato")
        case "oraBestSeller":
            print("sto eseguendo oraBestSeller")
            labelDescrizioneRicerca = Label(frameRicerca, text="Lo scopo di questa analisi è scoprire quali sono gli orari in cui vengono effettuati più ordini.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.oraBestSeller()
            tabella.heading("c1", text="Ora")
            tabella.heading("c2", text="Num.Prodotti venduti")

            fig = Figure(figsize=(5, 5), dpi=100)
            a = fig.add_subplot(111)
            x = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]
            y = risultato.map(lambda x: x[1]).collect()
            a.plot(x, y)
            canvas = FigureCanvasTkAgg(fig, master=frameGrafico)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=BOTH, expand=True)
            print("oraBestSeller - terminato")

        case "giornoBestSeller":
            print("sto eseguendo giornoBestSeller")
            labelDescrizioneRicerca = Label(frameRicerca, text="Lo scopo di questa analisi è scoprire quali sono i giorni in cui vengono effettuati più ordini.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", ackground="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.giornoBestSeller()
            tabella.heading("c1", text="Giorno")
            tabella.heading("c2", text="Num.Prodotti venduti")
            print("giornoBestSeller - terminato")
        case "topOraGiornoAcquistoUtente":
            print("sto eseguendo topOraGiornoAcquistoUtente")
            labelDescrizioneRicerca = Label(frameRicerca, text="Da sistemare.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.topOraGiornoAcquistoUtente()
            tabella = ttk.Treeview(frameElenco, column=("c1", "c2", "c3", "c4"), show='headings')
            tabella.heading("c1", text="ID Utente")
            tabella.heading("c2", text="Giorno")
            tabella.heading("c3", text="Ora")
            tabella.heading("c4", text="Num.Ordini effettuati")
            print("topOraGiornoAcquistoUtente - terminato")
        case "topProdottiRiordinatiPerGiorno":
            print("sto eseguendo topProdottiRiordinatiPerGiorno")
            labelDescrizioneRicerca = Label(frameRicerca, text="Da sistemare.\nDi seguito è possibile visualizzare un elenco completo e il relativo grafico", background="#ecbb06")
            labelDescrizioneRicerca.pack(ipady=30, ipadx=30, fill=X, pady=10)
            risultato = metodi.topProdottiRiordinatiPerGiorno()
            tabella = ttk.Treeview(frameElenco, column=("c1", "c2", "c3"), show='headings')
            tabella.heading("c1", text="ID Prodotto")
            tabella.heading("c2", text="Giorno")
            tabella.heading("c3", text="Quantità")
            print("topProdottiRiordinatiPerGiorno - terminato")

    tabella.pack(expand=True, fill=BOTH)

    for line in risultato.collect():
        tabella.insert('', END, values=line)


def opzioniRicercaAvanzata():

    comboBox = ttk.Combobox(frameRicerca, textvariable=query_selezionata)
    comboBox['values'] = 'readOnly'
    comboBox['values'] = ["topClientiOrdini", "topProdottiComprati", "ordiniPiuProdotti", "corridoioBestSeller", "oraBestSeller", "giornoBestSeller", "topOraGiornoAcquistoUtente", "topProdottiRiordinatiPerGiorno"]
    comboBox.pack(expand=True, fill=BOTH)
    #comboBox.bind("<<ComboboxSelected>>", risultatoElenco)

    buttonEseguiRicerca = Button(frameRicerca, text="Esegui una nuova analisi", background="#F5F5F7", command=risultatoElenco)
    buttonEseguiRicerca.pack(padx=10, pady=10)
    print("sono alla fine di ricerca avanzata")



def mostraGrafico():
    '''
    x = np.arange(0, 7)
    y = np.arange(0, 7)
    plt.plot(x, y)
    plt.title("Numero di prodotti venduti per giorno")
    plt.show()


'''
    # the figure that will contain the plot
    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    x = [1, 2, 4]
    y = [1, 2, 4]

    a.plot(x, y)

    canvas = FigureCanvasTkAgg(fig, master=frameGrafico)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)


#risultatoElenco(8)
#mostraGrafico()
opzioniRicercaAvanzata()
root.mainloop()


