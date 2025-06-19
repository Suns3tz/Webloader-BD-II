@echo off
echo 🔍 Verificando archivos en HDFS...

REM Verificar si namenode está corriendo
docker ps | findstr "namenode" >nul
if errorlevel 1 (
    echo ❌ El contenedor namenode no está ejecutándose
    exit /b 1
)

echo 📂 Contenido del directorio HDFS:
docker exec namenode hdfs dfs -ls /user/root/wiki_data

echo.
echo 📊 Información del archivo:
docker exec namenode hdfs dfs -du -h /user/root/wiki_data

echo.
echo 📄 Primeras líneas del archivo:
docker exec namenode hdfs dfs -cat /user/root/wiki_data/wiki_data.jsonl | head -3

echo.
echo ✅ Verificación completada