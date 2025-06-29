@echo off
echo Ejecutando pipeline completo de WebLoader desde cero...

REM === CONFIGURACIÓN Y VERIFICACIÓN INICIAL ===
echo Verificando requisitos del sistema...

REM Verificar que Docker está ejecutándose
docker version >nul 2>&1
if errorlevel 1 (
    echo Docker no está ejecutándose
    echo Inicia Docker Desktop primero
    pause
    exit /b 1
)

REM Verificar que docker-compose está disponible
docker-compose version >nul 2>&1
if errorlevel 1 (
    echo Docker Compose no está disponible
    pause
    exit /b 1
)

echo Docker verificado correctamente

REM === PASO 0: INICIAR CONTENEDORES CON DOCKER COMPOSE ===
echo.
echo Paso 0: Iniciando contenedores con Docker Compose...

REM Detener contenedores existentes si están corriendo
echo Deteniendo contenedores existentes...
docker-compose -f docker/hadoop/docker-compose.yml down 2>nul
docker-compose -f docker/spark-mysql/docker-compose.yml down 2>nul

REM Iniciar Hadoop
echo Iniciando Hadoop...
docker-compose -f docker/hadoop/docker-compose.yml up -d
if errorlevel 1 (
    echo Error iniciando Hadoop
    pause
    exit /b 1
)

REM Esperar que Hadoop esté listo
echo Esperando que Hadoop esté listo...
timeout /t 10 /nobreak >nul


REM Iniciar Spark y MySQL
echo Iniciando Spark y MySQL...
docker-compose -f docker/spark-mysql/docker-compose.yml up -d
if errorlevel 1 (
    echo Error iniciando Spark y MySQL
    pause
    exit /b 1
)

REM Esperar que los servicios estén listos
echo Esperando que todos los servicios estén listos...
timeout /t 10 /nobreak >nul

REM Verificar que todos los contenedores están ejecutándose
echo Verificando estado de contenedores...
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

REM Verificar contenedores específicos
docker ps | findstr "namenode" >nul
if errorlevel 1 (
    echo Namenode no está ejecutándose
    pause
    exit /b 1
)

docker ps | findstr "spark" >nul
if errorlevel 1 (
    echo Spark no está ejecutándose
    pause
    exit /b 1
)

docker ps | findstr "mysql" >nul
if errorlevel 1 (
    echo MySQL no está ejecutándose
    pause
    exit /b 1
)

echo Todos los contenedores están ejecutándose correctamente

REM === PASO 1: CONFIGURACIÓN COMPLETA DE BASE DE DATOS ===
echo.
echo Paso 1: Configuración completa de base de datos MySQL...

echo Esperando que MySQL esté completamente listo...
timeout /t 10 /nobreak >nul

REM Eliminar base de datos anterior si existe
echo Eliminando base de datos anterior...
docker exec mysql mysql -u root -proot -e "DROP DATABASE IF EXISTS proyecto02;" 2>nul

REM Crear base de datos y usuario
echo Creando base de datos y usuario...
docker exec mysql mysql -u root -proot -e "CREATE DATABASE proyecto02;"
if errorlevel 1 (
    echo Error creando base de datos
    pause
    exit /b 1
)

docker exec mysql mysql -u root -proot -e "CREATE USER IF NOT EXISTS 'pr'@'%%' IDENTIFIED BY 'pr';"
docker exec mysql mysql -u root -proot -e "GRANT ALL PRIVILEGES ON proyecto02.* TO 'pr'@'%%';"
docker exec mysql mysql -u root -proot -e "FLUSH PRIVILEGES;"
docker exec mysql mysql -u root -proot -e "SET GLOBAL log_bin_trust_function_creators = 1;"

REM Crear tablas
echo Creando tablas...
docker cp scripts\02_TABLES_PK_COMMENTS.sql mysql:/tmp/
docker cp scripts\03_INDEXES.sql mysql:/tmp/
docker cp scripts\04_QUERIES.sql mysql:/tmp/

docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/02_TABLES_PK_COMMENTS.sql"
docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/03_INDEXES.sql"
docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/04_QUERIES.sql"
if errorlevel 1 (
    echo Error creando tablas
    pause
    exit /b 1
)

echo Base de datos configurada correctamente
echo Verificando tablas creadas:
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;"

REM === PASO 2: EJECUTAR WEB CRAWLER ===
echo.
echo Paso 2: Ejecutando Web Crawler...
cd webCrawler
python uploader.py
if errorlevel 1 (
    echo Error ejecutando el web crawler
    cd ..
    pause
    exit /b 1
)
cd ..

REM Verificar que se crearon los datos en HDFS
echo Verificando datos en HDFS...
docker exec namenode hdfs dfs -test -e /user/root/wiki_data/wiki_data.jsonl
if errorlevel 1 (
    echo No se encontró el archivo wiki_data.jsonl en HDFS
    echo Verifica que el crawler haya subido los datos correctamente
    
    REM Mostrar contenido de HDFS para debug
    echo Contenido actual de HDFS:
    docker exec namenode hdfs dfs -ls /user/root/ 2>nul
    docker exec namenode hdfs dfs -ls /user/root/wiki_data 2>nul
    
    pause
    exit /b 1
)

echo Archivo wiki_data.jsonl encontrado en HDFS
echo Información del archivo:
docker exec namenode hdfs dfs -du -h /user/root/wiki_data
echo.

REM === PASO 3: EJECUTAR ANÁLISIS DE SPARK ===
echo.
echo Paso 3: Ejecutando análisis de Spark...

REM Copiar el script de análisis al contenedor de Spark
echo Copiando script de análisis...
docker cp spark_analyzer\spark_analyzer.py spark:/app/
if errorlevel 1 (
    echo Error copiando script al contenedor Spark
    pause
    exit /b 1
)

REM Instalar dependencias en el contenedor
echo Instalando dependencias en Spark...
docker exec spark pip install mysql-connector-python 2>nul
if errorlevel 1 (
    echo Warning: No se pudo instalar mysql-connector-python, continuando...
)

REM Ejecutar el análisis en el contenedor de Spark
echo Ejecutando análisis...
echo Procesando datos desde HDFS...
echo Guardando resultados en MySQL...
docker exec spark spark-submit --packages mysql:mysql-connector-java:8.0.33 /app/spark_analyzer.py
if errorlevel 1 (
    echo Error ejecutando análisis de Spark
    pause
    exit /b 1
)

echo Análisis de Spark completado exitosamente

REM === PASO 4: VERIFICACIÓN FINAL ===
echo.
echo Paso 4: Verificación final del sistema...

echo === ESTADO DE CONTENEDORES ===
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo.
echo === DATOS EN MYSQL ===
echo Verificando datos insertados:
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SELECT COUNT(*) as 'Páginas' FROM Page; SELECT COUNT(*) as 'Palabras' FROM Word; SELECT COUNT(*) as 'Relaciones Palabra-Página' FROM TopWordPages;"

echo.
echo === ARCHIVOS EN HDFS ===
docker exec namenode hdfs dfs -ls /user/root/wiki_data 2>nul
if errorlevel 1 (
    echo No se puede acceder a HDFS o no existen archivos
) else (
    echo Archivos encontrados en HDFS
    docker exec namenode hdfs dfs -du -h /user/root/wiki_data
)

echo.
echo === INTERFACES WEB DISPONIBLES ===
echo  Hadoop NameNode: http://localhost:9870
echo  Spark Master: http://localhost:8080
echo  WebLoader App: http://localhost:5000

echo.
echo Pipeline de datos completado exitosamente
echo Los datos han sido procesados y están disponibles en MySQL

REM === PASO 5: INICIAR APLICACIÓN WEB ===
echo.
echo Paso 5: Iniciando aplicación web...
echo La aplicación estará disponible en: http://localhost:5000
echo Presiona Ctrl+C para detener el servidor
echo.

cd WebLoader
python app.py

echo.
echo    Pipeline completo ejecutado
echo.
echo    Para detener todos los servicios ejecuta:
echo    docker-compose -f docker/hadoop/docker-compose.yml down
echo    docker-compose -f docker/spark-mysql/docker-compose.yml down

pause