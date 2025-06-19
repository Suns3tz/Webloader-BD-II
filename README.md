# Requisitos

### Sistema:
  - Windows 10 o superior / Linux
  - 16 GB de RAM
  - Procesador Intel i5 o superior
  - Al menos 100 GB de espacio en disco

### Dependencias que se van a instalar o necesitan:
  - Java 8
  - Python 3.8+
  - Docker
  - Docker Compose
  - MySQL 8.0+
  - Spark 3.5.6
  - Hadoop 3.2.1


# Instalación de Containers de Docker
Instalación de Containers de Docker:
Requerimientos:
- Java 8: https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html 
- Python 3.8: https://www.python.org/downloads/release/python-380/

### 1. Variables de entorno

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

### 2. Instalación docker-hadoop
  - Hacer git clone https://github.com/big-data-europe/docker-hadoop
  - Ingresar a la carpeta creada por GIT
  - Abrir una terminal dentro de la carpeta y ejecutar: docker-compose up -d

### 3. Instalación container Spark-MySQL
  - Ingresar a este Drive y descargar el archivo para el compose: https://drive.google.com/drive/folders/1qVH-F4cnPPLYHb_VbXpiWggKMsbdTV1N?usp=sharing
  ```yml
  version: '3.8'

services:
  # Servicio MySQL
  mysql:
    image: mysql:8.0
    container_name: mysql
    environment:
      MYSQL_ROOT_PASSWORD: root
      MYSQL_DATABASE: testdb
      MYSQL_USER: user
      MYSQL_PASSWORD: password
    ports:
      - "3307:3306"
    volumes:
      - mysql_data:/var/lib/mysql
    networks:
      - app-network

  # Servicio Spark
  spark:
    image: bitnami/spark:3.3.1
    container_name: spark
    hostname: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
      - SPARK_DAEMON_JAVA_OPTS=-Djava.library.path=/opt/bitnami/spark/lib
    ports:
      - "8080:8080"  # Spark UI
      - "7077:7077"  # Spark Master
    volumes:
      - ./spark-apps:/app
    networks:
      - app-network
    healthcheck:
      test: ["CMD", "ls", "/opt/bitnami/spark/lib"]
      interval: 10s
      timeout: 5s
      retries: 5

volumes:
  mysql_data:

networks:
  app-network:
    driver: bridge
  ```
  - Guardarlo en una carpeta 
  - Abrir una terminal dentro de la carpeta y ejecutar: docker-compose up -d

### 4. Pruebas
  - Para probar si los Container funcionan, recomiendo utilizar el código de [`prueba.py`](https://github.com/Suns3tz/Webloader-BD-II/blob/main/prueba.py) que está disponible en el Drive anterior o en el mismo repositorio.

