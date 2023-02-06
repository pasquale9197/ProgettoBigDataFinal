import tkinter
from tkinter import *
from tkinter import ttk
import tkinter as tk
import os
import sys
import main_pasquale as metodi
import tkinter.messagebox

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

logo = tkinter.PhotoImage(file="analysis.png")
label1 = Label(root, image=logo, background="#35363a")
label1.pack(ipady=10, ipadx=50)

def insertItemsListbox(listbox):

    listbox.insert(0, "topClientiOrdini (FAST)")
    listbox.insert(1, "ordiniPiuProdotti (FAST)")
    listbox.insert(2, "ordiniPerOra (FAST) + Grafico")
    listbox.insert(3, "ordiniPerGiorno (FAST) + Grafico")
    listbox.insert(4, "topOraGiornoAcquistoUtente (FAST)")
    listbox.insert(5, "prodottiComuniPiuAcquistati (FAST)")
    listbox.insert(6, "dipStessoCorridoioDiversiProd (FAST) + Grafico")
    listbox.insert(7, "prodottiAisle (FAST)")
    listbox.insert(8, "prodottiDepartment (FAST)")
    listbox.insert(9, "prodottiAisleDep (FAST)")
    listbox.insert(10, "ordiniUtente (FAST)")
    listbox.insert(11, "ordiniUtenteGiorno (FAST)")
    listbox.insert(12, "ordiniUtenteOra (FAST)")
    listbox.insert(13, "utenteUltimoOrdine (FAST)")
    listbox.insert(14, "daysSincePriorOrderUtente (FAST)")
    listbox.insert(15, "posizionePrioritaria (FAST)")
    listbox.insert(16, "aislesDepartmentsRiacquistati (FAST) + Grafico")
    listbox.insert(17, "numeroOrdine (FAST)")
    listbox.insert(18, "topProdottiComprati (MEDIUM)")
    listbox.insert(19, "venditePerCorridoio (MEDIUM) + Grafico")
    listbox.insert(20, "prodottoAcquistato (MEDIUM)")
    listbox.insert(21, "prodottoRiordinato (MEDIUM)")
    listbox.insert(22, "posizione (MEDIUM)")
    listbox.insert(23, "top15ProdottiWeekend (MEDIUM) + Grafico")
    listbox.insert(24, "prodottiSoloInTrain (MEDIUM)")
    listbox.insert(25, "prodottiInvenduti (MEDIUM)")
    listbox.insert(26, "top10ProdottiUtente (MEDIUM) + Grafico")
    listbox.insert(27, "topProdottiRiordinatiPerGiorno (SLOW)")
    listbox.insert(28, "top7ProdottiCorrelati (SLOW) + Grafico")
    listbox.insert(29, "confrontaVendite3Prodotti (VERY SLOW) + Grafico")
    listbox.insert(30, "variazioneGiornalieraTopFlop (VERY SLOW) + Grafico")

def results_topClientiOrdini(frameResults):

    print(">> Sto eseguendo results_topClientiOrdini")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.topClientiOrdini()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Utente")
    table.heading("c2", text="Ordini effettuati")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''

    print(">> Ho finito di eseguire results_topClientiOrdini")

def results_topProdottiComprati(frameResults):

    print(">> Sto eseguendo results_topProdottiComprati")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.topProdottiComprati()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Quantità Venduta")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_topProdottiComprati")

def results_ordiniPiuProdotti(frameResults):

    print(">> Sto eseguendo results_ordiniPiuProdotti")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.ordiniPiuProdotti()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Ordini")
    table.heading("c2", text="Quantità Prodotti Contenuta")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_ordiniPiuProdotti")

def results_venditePerCorridoio(frameResults):

    print(">> Sto eseguendo results_venditePerCorridoio")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.venditePerCorridoio()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Corridoio")
    table.heading("c2", text="Quantità Prodotti Venduta")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    y = np.array(results.map(lambda x: x[1]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.plot(y)

    plt.plot(y)
    plt.title("Vendite per corridoio")

    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_venditePerCorridoio")

def results_ordiniPerOra(frameResults):

    print(">> Sto eseguendo results_ordiniPerOra")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.ordiniPerOra()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Ora")
    table.heading("c2", text="Quantità Ordini Effetuati")
    table.pack(expand=True, fill=BOTH)

    r_sorted = results.sortBy(lambda x: x[1], ascending=False)

    for line in r_sorted.collect():
        table.insert('', END, values=line)

    x = np.arange(0, 24)
    y = np.array(results.map(lambda x: x[1]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.bar(x, y)

    plt.bar(x, y)
    plt.title("Ordini per ora")
    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_ordiniPerOra")

def results_ordiniPerGiorno(frameResults):

    print(">> Sto eseguendo results_ordiniPerGiorno")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.ordiniPerGiorno()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Giorno")
    table.heading("c2", text="Quantità Ordini Effetuati")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    x = np.array(["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"])
    y = np.array(results.map(lambda x: x[1]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.plot(x, y)

    plt.plot(x, y)
    plt.title("Vendite per giorno")
    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_ordiniPerGiorno")

def results_topOraGiornoAcquistoUtente(frameResults):

    print(">> Sto eseguendo results_topOraGiornoAcquistoUtente")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.topOraGiornoAcquistoUtente()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3", "c4"), show='headings')
    table.heading("c1", text="ID Utente")
    table.heading("c2", text="Giorno")
    table.heading("c3", text="Ora")
    table.heading("c4", text="Quantità Ordini Effetuati")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_topOraGiornoAcquistoUtente")

def results_topProdottiRiordinatiPerGiorno(frameResults):

    print(">> Sto eseguendo results_topProdottiRiordinatiPerGiorno")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.topProdottiRiordinatiPerGiorno()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''

    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="ID Prodotto")
    table.heading("c2", text="Giorno")
    table.heading("c3", text="Quantità Riordini")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_topProdottiRiordinatiPerGiorno")

def results_prodottiComuniPiuAcquistati(frameResults):

    print(">> Sto eseguendo results_prodottiComuniPiuAcquistati")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottiComuniPiuAcquistati()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Prodotto")
    table.heading("c2", text="Quantità Ordini")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottiComuniPiuAcquistati")

def results_dipStessoCorridoioDiversiProd(frameResults):

    print(">> Sto eseguendo results_dipStessoCorridoioDiversiProd")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.dipStessoCorridoioDiversiProd()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="Nome Dipartimento")
    table.heading("c2", text="Nome Corridoio")
    table.heading("c3", text="Quantità Prodotti")
    table.pack(expand=True, fill=BOTH)

    r_sorted = results.sortBy(lambda x: x[2], ascending=False)

    for line in r_sorted.collect():
        table.insert('', END, values=line)

    y = np.array(results.map(lambda x: x[2]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.plot(y)

    plt.plot(y)
    plt.title("Quantità prodotti diversi per stesso dip. e corridoio")
    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_dipStessoCorridoioDiversiProd")

def results_prodottiAisle(frameResults, aisle):

    print(">> Sto eseguendo results_prodottiAisle")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottiAisle(aisle)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
    '''
    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Nome Corridoio")
    table.heading("c3", text="Nome Dipartimento")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottiAisle")

def results_prodottiDepartment(frameResults, department):

    print(">> Sto eseguendo results_prodottiDepartment")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottiDepartment(department)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Nome Dipartimento")
    table.heading("c3", text="Nome Corridoio")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottiDepartment")

def results_prodottiAisleDep(frameResults, aisle, department):

    print(">> Sto eseguendo results_prodottiAisleDep")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottiAisleDep(aisle, department)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3", "c4"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="ID Prodotto")
    table.heading("c3", text="Nome Dipartimento")
    table.heading("c4", text="Nome Corridoio")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottiAisleDep")

def results_ordiniUtente(frameResults, user_id):

    print(">> Sto eseguendo results_ordiniUtente")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.ordiniUtente(user_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Utente")
    table.heading("c2", text="Quantità Ordini")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    x = np.array([user_id])
    y = np.array(results.map(lambda x: x[1]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.bar(x, y)

    plt.bar(x, y)
    plt.title("Quantità ordini per l'utente " + user_id)
    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_ordiniUtente")

def results_ordiniUtenteGiorno(frameResults, user_id, giorno):

    print(">> Sto eseguendo results_ordiniUtenteGiorno")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.ordiniUtenteGiorno(user_id, giorno)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Utente")
    table.heading("c2", text="Quantità Ordini")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_ordiniUtenteGiorno")

def results_ordiniUtenteOra(frameResults, user_id, ora):

    print(">> Sto eseguendo results_ordiniUtenteOra")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.ordiniUtenteOra(user_id, ora)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Utente")
    table.heading("c2", text="Quantità Ordini")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_ordiniUtenteOra")

def results_utenteUltimoOrdine(frameResults, user_id):

    print(">> Sto eseguendo results_utenteUltimoOrdine")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.utenteUltimoOrdine(user_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3", "c4"), show='headings')
    table.heading("c1", text="ID Ordine")
    table.heading("c2", text="Nome Prodotto")
    table.heading("c3", text="Posizione nell'ordine")
    table.heading("c4", text="Riordinato")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_utenteUltimoOrdine")

def results_daysSincePriorOrderUtente(frameResults, user_id):

    print(">> Sto eseguendo results_daysSincePriorOrderUtente")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.daysSincePriorOrderUtente(user_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1"), show='headings')
    table.heading("c1", text="Giorni dall'ultimo ordine")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_daysSincePriorOrderUtente")

def results_prodottoAcquistato(frameResults, product_id):

    print(">> Sto eseguendo results_prodottoAcquistato")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottoAcquistato(product_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Quantità Venduta")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottoAcquistato")

def results_prodottoRiordinato(frameResults, product_id):

    print(">> Sto eseguendo results_prodottoRiordinato")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottoRiordinato(product_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Quantità Riordinata")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottoRiordinato")

def results_top7ProdottiCorrelati(frameResults, product_name):

    print(">> Sto eseguendo results_top7ProdottiCorrelati")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.top7ProdottiCorrelati(product_name)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Quantità Acquistata")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    mylabels = np.array(results.map(lambda x: x[0]).collect())
    y = np.array(results.map(lambda x: x[1]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.pie(y, labels=mylabels, autopct='%1.1f%%', textprops={'fontsize': 8})

    plt.pie(y, labels=mylabels, autopct='%1.1f%%')
    plt.title("7 Prodotti più correlati a " + product_name)
    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_top7ProdottiCorrelati")

def results_posizione(frameResults, posizione):

    print(">> Sto eseguendo results_posizione")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.posizione(posizione)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Num. Ordini In Posizione")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_posizione")

def results_posizionePrioritaria(frameResults):

    print(">> Sto eseguendo results_posizionePrioritaria")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.posizionePrioritaria()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Prodotto")
    table.heading("c2", text="Posizione nell'ordine")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_posizionePrioritaria")

def results_aislesDepartmentsRiacquistati(frameResults):

    print(">> Sto eseguendo results_aislesDepartmentsRiacquistati")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.aislesDepartmentsRiacquistati()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="ID Corridoio")
    table.heading("c2", text="ID Dipartimento")
    table.heading("c3", text="Quantità Prodotti Riacquistati")
    table.pack(expand=True, fill=BOTH)

    r_sorted = results.sortBy(lambda x: x[2], ascending=False)

    for line in r_sorted.collect():
        table.insert('', END, values=line)

    y = np.array(results.map(lambda x: x[2]).collect())

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)
    a.plot(y)

    plt.plot(y)
    plt.title("Quantità prodotti diversi per stesso dip. e corridoio")
    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_aislesDepartmentsRiacquistati")

def results_numeroOrdine(frameResults, user_id, order_id):

    print(">> Sto eseguendo results_numeroOrdine")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.numeroOrdine(user_id, order_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)
    '''
    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)
'''
    table = ttk.Treeview(frameListResults, column=("c1"), show='headings')
    table.heading("c1", text="Numero Ordine")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_numeroOrdine")

def results_confrontaVendite3Prodotti(frameResults, product_name1, product_name2, product_name3):

    print(">> Sto eseguendo results_confrontaVendite3Prodotti")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results1 = metodi.prodottoSceltoDaUtente(product_name1)
    results2 = metodi.prodottoSceltoDaUtente(product_name2)
    results3 = metodi.prodottoSceltoDaUtente(product_name3)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c1", text="Giorno Ordine")
    table.heading("c1", text="Quantità Vendite")
    table.pack(expand=True, fill=BOTH)
    
    for line in results1.collect():
        table.insert('', END, values=line)
    for line in results2.collect():
        table.insert('', END, values=line)
    for line in results3.collect():
        table.insert('', END, values=line)

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)

    x = np.array(["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"])

    giorno_ordine = results1.map(lambda x: x[1]).collect()
    quantita_ordine = results1.map(lambda x: x[2]).collect()
    v = [0, 0, 0, 0, 0, 0, 0]

    for i in range(len(giorno_ordine)):
        v[int(giorno_ordine[i])] = int(quantita_ordine[i])

    y = np.array(v)
    a.plot(x, y)
    plt.plot(x, y, label=product_name1)

    giorno_ordine = results2.map(lambda x: x[1]).collect()
    quantita_ordine = results2.map(lambda x: x[2]).collect()
    v = [0, 0, 0, 0, 0, 0, 0]

    for i in range(len(giorno_ordine)):
        v[int(giorno_ordine[i])] = int(quantita_ordine[i])

    y = np.array(v)

    a.plot(x, y)
    plt.plot(x, y, label=product_name2)

    giorno_ordine = results3.map(lambda x: x[1]).collect()
    quantita_ordine = results3.map(lambda x: x[2]).collect()
    v = [0, 0, 0, 0, 0, 0, 0]

    for i in range(len(giorno_ordine)):
        v[int(giorno_ordine[i])] = int(quantita_ordine[i])

    y = np.array(v)

    a.plot(x, y)
    plt.plot(x, y, label=product_name3)

    plt.legend()
    plt.title("Confronto vendite giornaliere di 3 prodotti")

    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_confrontaVendite3Prodotti")


def results_variazioneGiornalieraTopFlop(frameResults):

    print(">> Sto eseguendo results_variazioneGiornalieraTopFlop")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results1 = metodi.prodottoPiuAcquistato()
    results2 = metodi.prodottoMenoAcquistato()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2", "c3"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Giorno Ordine")
    table.heading("c3", text="Quantità Vendite")
    table.pack(expand=True, fill=BOTH)

    for line in results1.collect():
        table.insert('', END, values=line)
    for line in results2.collect():
        table.insert('', END, values=line)

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)

    x = np.array(["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"])

    giorno_ordine = results1.map(lambda x: x[1]).collect()
    quantita_ordine = results1.map(lambda x: x[2]).collect()
    v = [0, 0, 0, 0, 0, 0, 0]

    for i in range(len(giorno_ordine)):
        v[int(giorno_ordine[i])] = int(quantita_ordine[i])

    y = np.array(v)

    a.plot(x, y)
    plt.plot(x, y, label="Più venduto")

    giorno_ordine = results2.map(lambda x: x[1]).collect()
    quantita_ordine = results2.map(lambda x: x[2]).collect()
    v = [0, 0, 0, 0, 0, 0, 0]

    for i in range(len(giorno_ordine)):
        v[int(giorno_ordine[i])] = int(quantita_ordine[i])

    y = np.array(v)

    a.plot(x, y)
    plt.plot(x, y, label="Meno venduto")

    plt.title("Confronto variazione vendite giornaliere"
              "\n prodotto più venduto e meno venduto")
    plt.legend()

    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_results_variazioneGiornalieraTopFlop")

def results_top15ProdottiWeekend(frameResults):

    print(">> Sto eseguendo results_top15ProdottiWeekend")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.top15ProdottiWeekend()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Quantità Vendite")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)

    x = np.array(results.map(lambda x: x[0]).collect())
    y = np.array(results.map(lambda x: x[1]).collect())

    a.barh(x, y)
    plt.yticks(fontsize=8)
    plt.barh(x, y)
    plt.title("Top 15 prodotti venduti nel weekend")

    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_top15ProdottiWeekend")

def results_prodottiSoloInTrain(frameResults):

    print(">> Sto eseguendo results_prodottiSoloInTrain")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottiSoloInTrain()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c1", text="Quantità Venduta")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottiSoloInTrain")

def results_prodottiInvenduti(frameResults):

    print(">> Sto eseguendo results_prodottiInvenduti")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.prodottiInvenduti()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="ID Prodotto")
    table.heading("c2", text="Nome Prodotto")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    print(">> Ho finito di eseguire results_prodottiInvenduti")

def results_variazioneOrdiniTrainPrior(frameResults):

    print(">> Sto eseguendo results_variazioneOrdiniTrainPrior")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results1 = metodi.variazioneOrderPrior()
    results2 = metodi.variazioneOrderTrain()

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Giorno")
    table.heading("c2", text="Quantità vendite")
    table.pack(expand=True, fill=BOTH)

    for line in results1.collect():
        table.insert('', END, values=line)

    for line in results2.collect():
        table.insert('', END, values=line)

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)

    x = np.array(["Lunedì", "Martedì", "Mercoledì", "Giovedì", "Venerdì", "Sabato", "Domenica"])
    y = np.array(results1.map(lambda x: x[1]).collect())
    a.plot(x, y)
    plt.plot(x, y)

    y = np.array(results2.map(lambda x: x[1]).collect())
    a.plot(x, y)
    plt.plot(x, y)

    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    print(">> Ho finito di eseguire results_variazioneOrdiniTrainPrior")

def results_top10ProdottiUtente(frameResults, user_id):

    print(">> Sto eseguendo results_top10ProdottiUtente")

    for widget in frameResults.winfo_children():
        widget.destroy()

    results = metodi.top10ProdottiUtente(user_id)

    frameListResults = Frame(frameResults, background="#3c3f41")
    frameListResults.pack(side=LEFT, expand=True, fill=BOTH, padx=5, pady=2)
    labelListResults = Label(frameListResults, text="ELENCO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelListResults.pack(ipady=10)

    frameGraphic = Frame(frameResults, background="#3c3f41")
    frameGraphic.pack(side=RIGHT, expand=True, fill=BOTH, padx=5, pady=2)
    labelGrafico = Label(frameGraphic, text="GRAFICO DEI RISULTATI", background="#3c3f41", foreground="white")
    labelGrafico.pack(ipady=10)

    table = ttk.Treeview(frameListResults, column=("c1", "c2"), show='headings')
    table.heading("c1", text="Nome Prodotto")
    table.heading("c2", text="Quantità")
    table.pack(expand=True, fill=BOTH)

    for line in results.collect():
        table.insert('', END, values=line)

    fig = Figure(figsize=(5, 5), dpi=100)
    a = fig.add_subplot(111)

    x = np.array(results.map(lambda x: x[0]).collect())
    y = np.array(results.map(lambda x: x[1]).collect())

    a.barh(x, y)
    plt.yticks(fontsize=8)
    plt.barh(x, y)
    plt.title("Top 10 prodotti acquistati da " + user_id)

    button = Button(frameGraphic, text="ZOOM GRAFICO", command=lambda: plt.show())
    button.pack(side=BOTTOM, expand=True, fill=X, pady=3)

    canvas = FigureCanvasTkAgg(fig, master=frameGraphic)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=BOTH, expand=True)

    print(">> Ho finito di eseguire results_top10ProdottiUtente")

def createFormBox(frame, querySelected, frameResults):

    for widgtet in frame.winfo_children():
        widgtet.destroy()

    match querySelected:

        case "topClientiOrdini (FAST)":

            print(">> Hai selezionato topClientiOrdini dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="topClientiOrdini:\n\n Restituisce il numero di ordini effettuati per ogni utente. I risultati sono "
                                                            "ordinati in ordine decrescente in base al numero di ordini.\n Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_topClientiOrdini(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "topProdottiComprati (MEDIUM)":

            print(">> Hai selezionato topProdottiComprati dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="topProdottiComprati:\n\n Restituisce la quantità venduta per ogni prodotto. I risultati sono "
                                                            "ordinati in ordine decrescente in base alla quantità venduta del prodotto.\n Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_topProdottiComprati(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "ordiniPiuProdotti (FAST)":

            print(">> Hai selezionato ordiniPiuProdotti dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="ordiniPiuProdotti:\n\n Restituisce la quantità di prodotti contenuta in ogni ordine.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_ordiniPiuProdotti(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "venditePerCorridoio (MEDIUM) + Grafico":

            print(">> Hai selezionato venditePerCorridoio dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="venditePerCorridoio:\n\n Restituisce la quantità di prodotti venduta per ogni corridoio.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_venditePerCorridoio(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "ordiniPerOra (FAST) + Grafico":

            print(">> Hai selezionato ordiniPerOra dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="ordiniPerOra:\n\n Restituisce la quantità di ordini effettuati per ogni ora.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_ordiniPerOra(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "ordiniPerGiorno (FAST) + Grafico":

            print(">> Hai selezionato ordiniPerGiorno dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="ordiniPerGiorno:\n\n Restituisce la quantità di ordini effettuati per ogni giorno.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_ordiniPerGiorno(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "topOraGiornoAcquistoUtente (FAST)":

            print(">> Hai selezionato topOraGiornoAcquistoUtente dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="topOraGiornoAcquistoUtente:\n\n Restituisce la quantità di ordini effettuati per ogni utente, giorno e ora.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_topOraGiornoAcquistoUtente(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "topProdottiRiordinatiPerGiorno (SLOW)":

            print(">> Hai selezionato topProdottiRiordinatiPerGiorno dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="topProdottiRiordinatiPerGiorno:\n\n Restituisce il numero di riordini di ogni prodotto per ogni giorno.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_topProdottiRiordinatiPerGiorno(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottiComuniPiuAcquistati (FAST)":

            print(">> Hai selezionato prodottiComuniPiuAcquistati dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottiComuniPiuAcquistati:\n\n Restituisce per ogni prodotto il numero di ordini in cui compare.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottiComuniPiuAcquistati(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "dipStessoCorridoioDiversiProd (FAST) + Grafico":

            print(">> Hai selezionato dipStessoCorridoioDiversiProd dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="dipStessoCorridoioDiversiProd:\n\n Ritorna il numero di prodotti diversi di uno stesso dipartimento all'interno di uno stesso corridoio.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_dipStessoCorridoioDiversiProd(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottiAisle (FAST)":

            print(">> Hai selezionato prodottiAisle dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottiAisle:\n\n Restituisce tutti i prodotti presenti in uno specifico corridoio.\n "
                                                            "É necessario inserire il nome di un corridoio per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="NOME CORRIDOIO", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottiAisle(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottiDepartment (FAST)":

            print(">> Hai selezionato prodottiDepartment dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottiDepartment:\n\n Restituisce tutti i prodotti appartenenti ad uno specifico dipartimento.\n "
                                                            "É necessario inserire il nome di un dipartimento per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="NOME DIPARTIMENTO", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottiDepartment(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottiAisleDep (FAST)":

            print(">> Hai selezionato prodottiAisleDep dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottiAisleDep:\n\n Ritorna tutti i prodotti appartenenti ad uno specifico corridoio e dipartimento.\n "
                                                            "É necessario inserire il nome di un dipartimento e di un corridoio per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="NOME CORRIDOIO", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True)
            text1 = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text1)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQuery2 = Frame(frame, background="#313335")
            frameFormQuery2.pack(expand=True, fill=X, padx=10, pady=5)
            label2 = Label(frameFormQuery2, text="NOME DIPARTIMENTO", background="#313335", foreground="white")
            label2.pack(side=LEFT, expand=True)
            text2 = tk.StringVar()
            entry2 = Entry(frameFormQuery2, textvariable=text2)
            entry2.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottiAisleDep(frameResults, entry1.get(), entry2.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "ordiniUtente (FAST)":

            print(">> Hai selezionato ordiniUtente dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="ordiniUtente:\n\n Restituisce tutti gli ordini di un utente specifico.\n "
                                                            "É necessario inserire l'id dell'utente specifico.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA",  command=lambda: results_ordiniUtente(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "ordiniUtenteGiorno (FAST)":

            print(">> Hai selezionato ordiniUtenteGiorno dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="ordiniUtenteGiorno:\n\n Restituisce tutti gli ordini di un utente in un giorno specifico.\n "
                                                            "É necessario inserire id dell'utente e giorno specifici per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True)
            text1= tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text1)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQuery2 = Frame(frame, background="#313335")
            frameFormQuery2.pack(expand=True, fill=X, padx=10, pady=5)
            label2 = Label(frameFormQuery2, text="GIORNO", background="#313335", foreground="white")
            label2.pack(side=LEFT, expand=True)
            text2 = tk.StringVar()
            entry2 = Entry(frameFormQuery2, textvariable=text2)
            entry2.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_ordiniUtenteGiorno(frameResults, entry1.get(), entry2.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "ordiniUtenteOra (FAST)":

            print(">> Hai selezionato ordiniUtenteOra dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="ordiniUtenteOra:\n\n Restituisce tutti gli ordini di un utente in un'ora specifica.\n "
                                                            "É necessario inserire id dell'utente e ora specifici per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True)
            text1 = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text1)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQuery2 = Frame(frame, background="#313335")
            frameFormQuery2.pack(expand=True, fill=X, padx=10, pady=5)
            label2 = Label(frameFormQuery2, text="ORA", background="#313335", foreground="white")
            label2.pack(side=LEFT, expand=True)
            text2 = tk.StringVar()
            entry2 = Entry(frameFormQuery2, textvariable=text2)
            entry2.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_ordiniUtenteOra(frameResults, entry1.get(), entry2.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "utenteUltimoOrdine (FAST)":

            print(">> Hai selezionato utenteUltimoOrdine dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="utenteUltimoOrdine:\n\n Restituisce l'ultimo ordine per uno specifico utente.\n "
                                                            "É necessario inserire l'id dell'utente per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_utenteUltimoOrdine(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "daysSincePriorOrderUtente (FAST)":

            print(">> Hai selezionato daysSincePriorOrderUtente dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="daysSincePriorOrderUtente:\n\n Restituisce il tempo intercorso tra l'ultimo ordine di uno specifico utente e quello precedente da lui effettuato.\n "
                                                            "É necessario inserire l'id dell'utente per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_daysSincePriorOrderUtente(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottoAcquistato (MEDIUM)":

            print(">> Hai selezionato prodottoAcquistato dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottoAcquistato:\n\n Ritorna il numero di volte che il prodotto specifico è stato acquistato.\n "
                                                            "É necessario inserire l'id del prodotto per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID PRODOTTO", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottoAcquistato(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottoRiordinato (MEDIUM)":

            print(">> Hai selezionato prodottoRiordinato dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottoRiordinato:\n\n Ritorna il numero di volte che il prodotto specifico è stato riordinato.\n "
                                                            "É necessario inserire l'id del prodotto per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID PRODOTTO", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottoRiordinato(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "top7ProdottiCorrelati (SLOW) + Grafico":

            print(">> Hai selezionato top7ProdottiCorrelati dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="top7ProdottiCorrelati:\n\n Controlla l'eventuale correlazione tra prodotti acquistati. Nello specifico, dato un prodotto P"
                                   "\ncerca quali sono i prodotti più acquistati insieme a P. L'analisi restituisce la top 7 dei prodotti correlati.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="NOME PRODOTTO", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_top7ProdottiCorrelati(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "posizione (MEDIUM)":

            print(">> Hai selezionato posizione dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="posizione:\n\n Data una posizione, restituisce tutti i prodotti in quella posizione in tutti gli ordini.\n "
                                                            "É necessario inserire una posizione per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="POSIZIONE NELL'ORDINE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_posizione(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "posizionePrioritaria (FAST)":

            print(">> Hai selezionato posizionePrioritaria dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="posizionePrioritaria:\n\n Restituisce quali sono gli alimenti con più priorità acquistati, ovvero quelli che sono stati aggiunti per primi nel carrello (prima posizione).\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_posizionePrioritaria(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "aislesDepartmentsRiacquistati (FAST) + Grafico":

            print(">> Hai selezionato AislesDepartmentsRiacquistati dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="aislesDepartmentsRiacquistati:\n\n Restituisce i corridoi e i dipartimenti in cui sono stati riacquistati prodotti.\n "
                                                            "Non è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_aislesDepartmentsRiacquistati(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "numeroOrdine (FAST)":

            print(">> Hai selezionato numeroOrdine dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="numeroOrdine:\n\n Dato uno specifico utente e uno specifico ordine restituisce il numero di quell'ordine.\n "
                                                            "É necessario inserire l'id utente e l'id dell'ordine per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True)
            text1 = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text1)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQuery2 = Frame(frame, background="#313335")
            frameFormQuery2.pack(expand=True, fill=X, padx=10, pady=5)
            label2 = Label(frameFormQuery2, text="ID ORDINE", background="#313335", foreground="white")
            label2.pack(side=LEFT, expand=True)
            text2 = tk.StringVar()
            entry2 = Entry(frameFormQuery2, textvariable=text2)
            entry2.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_numeroOrdine(frameResults, entry1.get(), entry2.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "confrontaVendite3Prodotti (VERY SLOW) + Grafico":

            print(">> Hai selezionato confrontaVendite3Prodotti dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="confrontaVendite3Prodotti:\n\n L'utente ha la possibilità di inserire il nome di 3 prodotti "
                                                            "\ndi cui vuole confrontare l'andamento delle vendite giornaliere.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="PRODOTTO 1", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True)
            text1 = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text1)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQuery2 = Frame(frame, background="#313335")
            frameFormQuery2.pack(expand=True, fill=X, padx=10, pady=5)
            label2 = Label(frameFormQuery2, text="PRODOTTO 2", background="#313335", foreground="white")
            label2.pack(side=LEFT, expand=True)
            text2 = tk.StringVar()
            entry2 = Entry(frameFormQuery2, textvariable=text2)
            entry2.pack(side=RIGHT, expand=True, fill=X)

            frameFormQuery3 = Frame(frame, background="#313335")
            frameFormQuery3.pack(expand=True, fill=X, padx=10, pady=5)
            label3 = Label(frameFormQuery3, text="PRODOTTO 3", background="#313335", foreground="white")
            label3.pack(side=LEFT, expand=True)
            text3 = tk.StringVar()
            entry3 = Entry(frameFormQuery3, textvariable=text3)
            entry3.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_confrontaVendite3Prodotti(frameResults, entry1.get(), entry2.get(), entry3.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "variazioneGiornalieraTopFlop (VERY SLOW) + Grafico":

            print(">> Hai selezionato variazioneGiornalieraTopFlop dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="variazioneGiornalieraTopFlop:\n\n L'analisi restituisce il prodotto più venduto in assoluto e quello meno venduto. Mettendo a confronto la variazione giornaliera delle vendite dei due prodotti."
                                                            "\nNon è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_variazioneGiornalieraTopFlop(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "top15ProdottiWeekend (MEDIUM) + Grafico":

            print(">> Hai selezionato top15ProdottiWeekend dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc,
                                   text="top15ProdottiWeekend:\n\n L'analisi restituisce i 15 prodotti più vednuti nel fine settimana. Ad esempio questo potrebbe far intuire che le abitudini di acquisto differiscono rispetto"
                                        "\n ai giorni lavorativi in quanto i clienti hanno più tempo per cucinare."
                                        "\nNon è necessario inserire nessun parametro per l'analisi.",
                                   background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_top15ProdottiWeekend(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottiSoloInTrain (MEDIUM)":

            print(">> Hai selezionato prodottiSoloInTrain dalla lista")
            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc,
                                   text="prodottiSoloInTrain:\n\n L'analisi restituisce i prodotti presenti solo negli ultimi ordini degli utenti, questo potrebbe far pensare che"
                                        "\nnel negozio sono stati introdotti nuovi prodotti (perchè acquistati solo negli ultimi ordini) e capirne l'apprezzamento rispetto"
                                        "\n ai giorni lavorativi in cui i clienti hanno più tempo per cucinare."
                                        "\nNon è necessario inserire nessun parametro per l'analisi.",
                                   background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottiSoloInTrain(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "prodottiInvenduti (MEDIUM)":

            print(">> Hai selezionato prodottiInvenduti dalla lista")

            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="prodottiInvenduti:\n\n L'analisi restituisce i prodotti che non appaiono in nessun ordine, ovvero prodotti inveduti."
                                        "\nNon è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_prodottiInvenduti(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "variazioneOrdiniTrainPrior (MEDIUM)":

            print(">> Hai selezionato variazioneOrdiniTrainPrior dalla lista")

            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="variazioneOrdiniTrainPrior:\n\n L'analisi restituisce la variazione delle vendite giornaliere dei prodotti presenti in prior e viene confrontata con quella dei prodotti presenti in train"
                                        "\nNon è necessario inserire nessun parametro per l'analisi.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_variazioneOrdiniTrainPrior(frameResults))
            button.pack(side=RIGHT, expand=True, fill=X)

        case "top10ProdottiUtente (MEDIUM) + Grafico":

            print(">> Hai selezionato top10ProdottiUtente dalla lista")

            frameFormQueryDesc = Frame(frame, background="#3b4754")
            frameFormQueryDesc.pack(expand=True, fill=X, padx=10, pady=10)
            labelQueryDesc = Label(frameFormQueryDesc, text="top10ProdottiUtente:\n\n L'analisi restituisce i 10 prodotti più acquistati da un cliente specifico."
                                        "\nÉ necessario inserire l'id di un utente.", background="#ecbb06")
            labelQueryDesc.pack(ipady=30, ipadx=30, fill=X, pady=5)

            frameFormQuery1 = Frame(frame, background="#313335")
            frameFormQuery1.pack(expand=True, fill=X, padx=10, pady=5)
            label1 = Label(frameFormQuery1, text="ID UTENTE", background="#313335", foreground="white")
            label1.pack(side=LEFT, expand=True, fill=X)
            text = tk.StringVar()
            entry1 = Entry(frameFormQuery1, textvariable=text)
            entry1.pack(side=RIGHT, expand=True, fill=X)

            frameFormQueryButton = Frame(frame)
            frameFormQueryButton.pack(expand=True, fill=X, padx=10, pady=5)
            button = Button(frameFormQueryButton, text="ESEGUI RICERCA", command=lambda: results_top10ProdottiUtente(frameResults, entry1.get()))
            button.pack(side=RIGHT, expand=True, fill=X)

def createSearchBox():

    frameSearch = Frame(root, border=10, background="#3b4754")
    frameSearch.pack(side=TOP, fill=X, padx=50, pady=10)

    #left side of search box
    frameListQuery = Frame(frameSearch, background="#313335")
    frameListQuery.pack(side=LEFT, fill=X, padx=10, pady=10)

    labelListQuery = Label(frameListQuery, text="ELENCO QUERY DISPONIBILI", background="#313335", foreground="white")
    labelListQuery.pack(pady=5)

    listBoxQuery = Listbox(frameListQuery, width=60, background="#f0f0f0")
    insertItemsListbox(listBoxQuery)
    listBoxQuery.pack(expand=True, fill=X, pady=5)

    #right side of search box
    frameFormQuery = Frame(frameSearch, background="#3b4754")
    frameFormQuery.pack(fill=X, padx=10, pady=10)

    #results
    frameResults = Frame(root, background="#313335", border=10)
    frameResults.pack(expand=True, fill=BOTH, padx=50)

    #querySelected = listBoxQuery.get(ANCHOR)
    listBoxQuery.bind('<<ListboxSelect>>', lambda x: createFormBox(frameFormQuery, listBoxQuery.get(ANCHOR), frameResults))

createSearchBox()
root.mainloop()