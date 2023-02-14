import findspark
import numpy as np 
import random as rn
import sys

findspark.init("/opt/spark")
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("DSPSO").config("spark.executor.memory","1gb").getOrCreate()
sc = spark.sparkContext

def slave1(fit, fit_min, posicion, mejor_posicion):
    if fit < fit_min:
        return posicion
    else:
        return mejor_posicion

def slave2(fit, fit_min):
    if fit < fit_min:
        return fit
    else:
        return fit_min

def MSE(y, pred):
    n = len(y)
    if n != len(pred):
        print("error: datos y predicción de distintos tamaños")
        return -1

    resultado = 0.0

    for i in range(n):
        resultado += (y[i] - pred[i])**2

    resultado = resultado / n

    return resultado

V_max = 10
W = 1
c_1 = 0.3
c_2 = 0.7


objetivo = [50, 50, 50]


n = 50000  # número de  iteraciones
m = 10  # número de partículas

posiciones = []
velocidades = []
mejores_pos_locales = []
best_local_fitness =[]

#máximo float
max = sys.float_info.max
 

#Inicializamos los vectores

for i in range(m):
    aux = [rn.uniform(-100,100), rn.uniform(-100,100), rn.uniform(-100,100)]
    posiciones.append(aux)
    # mejores_pos_locales.append(aux)
    velocidades.append([rn.uniform(-100,100), rn.uniform(-100,100), rn.uniform(-100,100)])
    # best_local_fitness.append(max)

mejor_pos_global = posiciones[0]
best_global_fitness = max

RDD_main = sc.parallelize(posiciones)
#RDD_main = RDD_main.collect()
#RDD_main = RDD_main.map(lambda x: x**2 )
#RDD_main = RDD_main.map(lambda x: (list(x), list(x), max) )
RDD_main = RDD_main.map(lambda x: (list(x), list(x), MSE(x, objetivo), best_global_fitness, velocidades))


for i in range(n):
    #Actualizamos el MSE
    RDD_main = RDD_main.map(lambda x: (x[0], x[1], MSE(x[0], objetivo), best_global_fitness, velocidades))
    #controlamos el mejor ajuste local y mejor posición local
    RDD_main = RDD_main.map(lambda x: (x[0], slave1(x[2], x[3], x[0], x[1]), x[2], slave2(x[2], x[3]), x[4] ))










RDD_main = RDD_main.collect()
RDD_main