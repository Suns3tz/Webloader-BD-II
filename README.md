# Webloader-BD-II
Instalación de Containers de Docker:
Requerimientos:
- Java 8: https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html 
- Python 3.8: https://www.python.org/downloads/release/python-380/

1. Variables de entorno
  - En Variables de Usuario o del Sistema colocar: 
    - JAVA_HOME: C:\Program Files\Java\jdk1.8.0_202
    - Dentro de PATH (tomen en cuenta el PATH si lo hicieron dentro de Usuario o del Sistema), colocar %JAVA_HOME%\bin
    - También, dentro del PATH, colocar: 
        - C:\Users\usuario\AppData\Local\Programs\Python\Python38\
        - C:\Users\usuario\AppData\Local\Programs\Python\Python38\Scripts\
  - De ser necesario (no sé si lo es): deben ingresar lo siguiente:
    - PYSPARK_HOME: C:\Users\usuario\AppData\Local\Programs\Python\Python38
    - SPARK_HOME: C:\spark\spark-3.5.6-bin-hadoop3
    - HADOOP_HOME: C:\hadoop

2. Instalación docker-hadoop
  - Hacer git clone https://github.com/big-data-europe/docker-hadoop
  - Ingresar a la carpeta creada por GIT
  - Abrir una terminal dentro de la carpeta y ejecutar: docker-compose up -d

3. Instalación container Spark-MySQL
  - Ingresar a este Drive y descargar el archivo para el compose: https://drive.google.com/drive/folders/1qVH-F4cnPPLYHb_VbXpiWggKMsbdTV1N?usp=sharing
  - Guardarlo en una carpeta 
  - Abrir una terminal dentro de la carpeta y ejecutar: docker-compose up -d

4. Pruebas
  - Para probar si los Container funcionan, recomiendo utilizar el código de python que está disponible en el Drive anterior

