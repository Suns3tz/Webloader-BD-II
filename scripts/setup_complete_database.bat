@echo off
echo 🗄️ Configuración completa de base de datos MySQL...

REM Verificar si MySQL está ejecutándose
docker ps | findstr "mysql" >nul
if errorlevel 1 (
    echo ❌ El contenedor MySQL no está ejecutándose
    echo 💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d
    exit /b 1
)

echo ⏳ Esperando que MySQL esté completamente listo...
timeout /t 10 /nobreak >nul

REM Paso 1: Verificar y crear base de datos y usuario
echo 📊 Paso 1: Configurando usuario y base de datos...

REM Verificar si la base de datos existe
docker exec mysql mysql -u root -proot -e "SHOW DATABASES LIKE 'proyecto02';" | findstr "proyecto02" >nul
if errorlevel 1 (
    echo 🔨 Creando base de datos proyecto02...
    docker exec mysql mysql -u root -proot -e "CREATE DATABASE proyecto02;"
    if errorlevel 1 (
        echo ❌ Error creando base de datos
        exit /b 1
    )
    echo ✅ Base de datos proyecto02 creada
) else (
    echo ✅ Base de datos proyecto02 ya existe
)

REM Verificar si el usuario existe
docker exec mysql mysql -u root -proot -e "SELECT User FROM mysql.user WHERE User='pr';" | findstr "pr" >nul
if errorlevel 1 (
    echo 🔨 Creando usuario pr...
    docker exec mysql mysql -u root -proot -e "CREATE USER 'pr'@'%%' IDENTIFIED BY 'pr';"
    if errorlevel 1 (
        echo ❌ Error creando usuario
        exit /b 1
    )
    echo ✅ Usuario pr creado
) else (
    echo ✅ Usuario pr ya existe
)

REM Asegurar permisos
echo 🔐 Asignando permisos al usuario pr...
docker exec mysql mysql -u root -proot -e "GRANT ALL PRIVILEGES ON proyecto02.* TO 'pr'@'%%';"
docker exec mysql mysql -u root -proot -e "FLUSH PRIVILEGES;"

REM Paso 2: Verificar y crear tablas
echo 📊 Paso 2: Configurando tablas...

REM Verificar si las tablas existen
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;" 2>nul | findstr "Word" >nul
if errorlevel 1 (
    echo 🔨 Creando tablas...
    
    REM Copiar script SQL al contenedor y ejecutarlo
    docker cp scripts\02_TABLES_PK_COMMENTS.sql mysql:/tmp/
    docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/02_TABLES_PK_COMMENTS.sql"
    
    if errorlevel 1 (
        echo ❌ Error creando tablas
        exit /b 1
    )
    echo ✅ Tablas creadas exitosamente
) else (
    echo ✅ Las tablas ya existen
)

REM Paso 3: Verificación final
echo 🔍 Paso 3: Verificación final...
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;"

if errorlevel 1 (
    echo ❌ Error en la verificación final
    exit /b 1
)

echo ✅ Base de datos configurada correctamente
echo 📊 Usuario: pr
echo 📊 Contraseña: pr
echo 📊 Base de datos: proyecto02
echo 📊 Puerto: 3307