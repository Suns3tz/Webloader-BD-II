@echo off
echo 🔄 Reiniciando base de datos completamente desde cero...

REM Verificar si MySQL está ejecutándose
docker ps | findstr "mysql" >nul
if errorlevel 1 (
    echo ❌ El contenedor MySQL no está ejecutándose
    echo 💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d
    exit /b 1
)

echo ⏳ Esperando que MySQL esté listo...
timeout /t 15 /nobreak >nul

echo 🗑️ Eliminando base de datos anterior...
docker exec mysql mysql -u root -proot -e "DROP DATABASE IF EXISTS proyecto02;" 2>nul

echo 🔨 Creando base de datos y usuario...
docker exec mysql mysql -u root -proot -e "CREATE DATABASE proyecto02;"
docker exec mysql mysql -u root -proot -e "CREATE USER IF NOT EXISTS 'pr'@'%%' IDENTIFIED BY 'pr';"
docker exec mysql mysql -u root -proot -e "GRANT ALL PRIVILEGES ON proyecto02.* TO 'pr'@'%%';"
docker exec mysql mysql -u root -proot -e "FLUSH PRIVILEGES;"

echo 📊 Creando tablas...
docker cp scripts\02_TABLES_PK_COMMENTS.sql mysql:/tmp/
docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/02_TABLES_PK_COMMENTS.sql"

if errorlevel 1 (
    echo ❌ Error creando tablas
    exit /b 1
)

echo ✅ Base de datos reiniciada completamente
echo 📊 Verificando tablas creadas:
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;"