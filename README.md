# LogParser

El código en este repositorio utiliza PySpark para procesar archivos CSV que contienen registros de conexión entre hosts. 
El script verifica si hay nuevos archivos en una carpeta "logs/", para posteriormente procesarlos obteniendo respuesta a 3 preguntas:

- Lista de nombres de host conectados al host dado como parámetro durante la última hora
- Lista de nombres de host que recibieron conexiones desde un host dado como parámetro durante la última hora
- El nombre de host que generó la mayor cantidad de conexiones en la última hora.

al finalizar la ejecución, mueve los archivos procesados a la carpeta "processed_logs/"

## Cosas importantes a cosiderar:
 1. El codigó asumen que el script y el directorio de logs se encuentran en el mismo directorio. Asegúrese de ajustar las rutas en consecuencia.
 2. Es mejor utilizar un scheduler como crontab, airflow o incluso una función en la nube para programar este script en lugar de utilizar time.sleep(3600) que hará que el contenedor se ejecute de manera indefinida.
 3. El código anterior funciona mejor en una configuración de clúster. Debido a que el método collect trae todos los datos al nodo del controlador y para grandes conjuntos de datos puede causar problemas de memoria.
 4. Los datos de los logs proporcionados tienen fechas anteriores a la ultima hora actual por los cual no los va a tomar en cuenta a la hora de ejecutar las operaciones del script; Se deben ingresar logs con fechas actualizadas o cambiar las lineas 19-20 (solución 1), lines 8-9 (solución 2).

Se tiene dos soluciones en el repostiorio:

### Solución 1:
Define funciones para:
 1. get_last_hour_records: para filtrar las conexiones de la última hora
 2. parse_log: utiliza el esquema para leer el archivo CSV, filtra las conexiones de la última hora y genera los informes sobre las conexiones

### Solución 2:
Define funciones para:
 1. map_csv_data: mapea los datos del archivo CSV a una tupla de (tiempo de epoch, host1, host2)
 2. get_hosts_connections: obtener una lista de nombres de host conectados desde o hacia un host específico en la último hora
 3. get_most_connected_host: obtener el host que generó la mayor cantidad de conexiones en el último hora. }
