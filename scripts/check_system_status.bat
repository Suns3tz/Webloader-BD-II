@echo off
echo 🔍 Verificando estado completo del sistema...

echo 📊 === CONTENEDORES DOCKER ===
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo.
echo 📂 === ARCHIVOS EN HDFS ===
docker exec namenode hdfs dfs -ls /user/root/wiki_data 2>nul
if errorlevel 1 (
    echo ❌ No se puede acceder a HDFS o no existen archivos
) else (
    echo ✅ Archivos encontrados en HDFS
)

echo.
echo 💾 === BASE DE DATOS MYSQL ===
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;" 2>nul
if errorlevel 1 (
    echo ❌ No se puede acceder a MySQL
) else (
    echo ✅ MySQL accesible
)

echo.
echo 🌐 === INTERFACES WEB DISPONIBLES ===
echo 📊 Hadoop NameNode: http://localhost:9870
echo ⚡ Spark Master: http://localhost:8080
echo 🌐 WebLoader App: http://localhost:5000

pause