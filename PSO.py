import numpy as np 
import random as rn

#versión inicial con un objetivo genérico

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
 

#Inicializamos los vectores

for i in range(m):
    aux = [rn.uniform(-100,100), rn.uniform(-100,100), rn.uniform(-100,100)]
    posiciones.append(aux)
    velocidades.append([rn.uniform(-100,100), rn.uniform(-100,100), rn.uniform(-100,100)])
    mejores_pos_locales.append(aux)

mejor_pos_global = posiciones[0]


#bucle principal
for i in range(n):
    for j in range(m):
        fit = MSE(posiciones[j], objetivo)

        if i == 0:
            best_local_fitness.append(fit)
            if j == 0:
                best_global_fitness = fit
            elif fit < best_global_fitness:
                best_global_fitness = fit
                mejor_pos_global = posiciones[j][:]
        else:
            if fit < best_local_fitness[j]:
                best_local_fitness[j] = fit
                mejores_pos_locales[j] = posiciones[j][:]
                if fit < best_global_fitness:
                    best_global_fitness = fit
                    mejor_pos_global = posiciones[j][:]
    
    r_1 = rn.random()
    r_2 = rn.random()
    for j in range(m):
        for k in range(3):
            velocidades[j][k] = W*velocidades[j][k] + c_1*r_1*(mejores_pos_locales[j][k] - posiciones[j][k]) + c_2*r_2*(mejor_pos_global[k] - posiciones[j][k])
            if velocidades[j][k] > V_max:
                velocidades[j][k] = V_max
            elif velocidades[j][k] < -V_max:
                velocidades[j][k] = -V_max
                
            posiciones[j][k] = posiciones[j][k] + velocidades[j][k]
            
print("mejor posición encontrada", mejor_pos_global)
print("mejor MSE", best_global_fitness)

        
        