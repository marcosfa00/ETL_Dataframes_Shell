# Practicando Pandas, ETL con Visual Studio Code
"""
Primero vamos a jugar con los dataframes leyendo de CSV pro ejemplo
"""
# El primer paso siempre es jhacer los imports
import sys           # Read system parameters.
import  pandas as pd # Manipulate and analyze data.
import sqlite3       # Manage SQL databases.

# Mostramos por consola los implorts realizados:

# Summarize software libraries used.
print('Libraries used in this project:')
print('- Python {}'.format(sys.version))
print('- pandas {}'.format(pd.__version__))
print('- sqlite3 {}'.format(sqlite3.sqlite_version))
# Ajustar la configuración de visualización de pandas
pd.set_option('display.max_columns', None)  # Mostrar todas las columnas
pd.set_option('display.width', None)         # No limitar el ancho de las columnas
pd.set_option('display.max_colwidth', None)  # Mostrar todo el contenido de las columnas


dataframe_loan_complaints = pd.read_csv('./consumer_loan_complaints.csv')
result =dataframe_loan_complaints.head()

# Mostrar el resultado
print(result)

print("----------------------------------")
print("ahora vamos a crear un dataframe solo con la información que nos interesa")
print("vamos a tratar de mostrar el número de quejas por usuario")

# primero devemos de crear una vista/dataframe con solo los datos que nos interesan

# Obtener una lista de códigos postales únicos y contar el número de quejas para cada código postal
quejas_por_codigo_postal = dataframe_loan_complaints['ZIP code'].value_counts()


# Mostrar el resultado
print(quejas_por_codigo_postal)


# ahora vamos a mostrar por cada código psotal el id de los usuaros pertenecientes a dicha Zona

user_per_ZIP =  dataframe_loan_complaints.groupby('ZIP code')['user_id'].apply(lambda x: ', '.join(x.unique()))

print(user_per_ZIP)
print(user_per_ZIP.shape)
# vamos a continuar con una base de datos, de esta manera las consultas SQL son más fáciles

conn = sqlite3.connect('./user_data.db')
# mostramos la talba users

users_query = "SELECT * FROM users"

dataframe_users = pd.read_sql(users_query, conn)

print("DATAFRAME USERS SQL")
print(dataframe_users.head())

# a partir de ahora la dinamica es un poquito diferente, se deberán hacer unas consultas SQL para obtener los datos que precisemos, mas adelante
# ya podremos trabajar con dataframes para obtener solo los datos necesarios.

# por ejemplo vamos a crear una vista donde tengamos solo la edad el trabajo y la duración.
query1 = "SELECT age,job,duration FROM users"
# ahora obtenemos el dataframe
dataframe_age_job = pd.read_sql(query1,conn)
# mostramos el dataframe
print("EDAD TRABAJO DURACION")
print(dataframe_age_job.head(n=3))


"""RESULTADO DE LA QUERY


   age           job  duration
0   58    management       261
1   44    technician       151
2   33  entrepreneur        76

AHORA APLICANDO LO APRENDIDO PODRÍAMOS ESTABLECER UNA RELACIÓN ENTRE LA EDAD Y LA DURACIÓN EN EL TRABAJO EN FUNCIÓN DEL TIPO DE TRABAJO PARA SABER CUANTO SUELEN DURAR DE MEDIA ESTAS 
PERSONAS EN UN TRABAJO
"""

query2 = "SELECT age,job,duration FROM users where job='entrepreneur'"
dataframe_entr = pd.read_sql(query2,conn)
print("ENTREPRENEUR AGE AND DURATION ")
print(dataframe_entr.head(n=6))
# vamos a coger dos clusters como referencia, estos pueden ser cualesquiera, pero vamos a coger el que tenga mayor edad y el que tenga menor edad
old = dataframe_entr['age'].idxmax()
oldest = dataframe_entr['age'].iloc[old]
# mostramos el dato
print("mayor: "+str(oldest))
young = dataframe_entr['age'].idxmin()
youngest = dataframe_entr['age'].iloc[young]
# mostramos el dato
print("menor: "+str(youngest))

counter = 0
# ahora voy a tratar de recorrer toda la base de datos y comparar estos valores, de esta manera podré obtener una estimación
for index, row in dataframe_entr.iterrows():

    age = row['age']
    work = row['job']
    duration = row['duration']

    if age > 50:
        counter+=1


# mostramos el dataframe con todas las personas mayores de 50 años
print( "total de personas mayores de 50 años= " + str(counter))










