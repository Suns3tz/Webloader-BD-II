@echo off
echo 📊 Verificando configuración de base de datos MySQL...

REM Verificar si MySQL está ejecutándose
docker ps | findstr "mysql" >nul
if errorlevel 1 (
    echo ❌ El contenedor MySQL no está ejecutándose
    echo 💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d
    exit /b 1
)

echo 🔍 Verificando conexión a la base de datos...
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;" >nul 2>&1
if errorlevel 1 (
    echo ❌ No se puede conectar a la base de datos proyecto02 con usuario 'pr'
    echo 💡 Verifica que la base de datos y el usuario estén creados correctamente
    exit /b 1
)

echo ✅ Base de datos verificada correctamente
echo 📊 Usuario: pr
echo 📊 Base de datos: proyecto02
echo 📊 Tablas disponibles:
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;"