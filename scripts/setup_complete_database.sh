#!/bin/bash

echo "🗄️ Configuración completa de base de datos MySQL..."

# Verificar si MySQL está ejecutándose
if ! docker ps | grep -q "mysql"; then
    echo "❌ El contenedor MySQL no está ejecutándose"
    echo "💡 Ejecuta: docker-compose -f docker/spark-mysql/docker-compose.yml up -d"
    exit 1
fi

echo "⏳ Esperando que MySQL esté completamente listo..."
sleep 10

# Paso 1: Verificar y crear base de datos y usuario
echo "📊 Paso 1: Configurando usuario y base de datos..."

# Verificar si la base de datos existe
if ! docker exec mysql mysql -u root -proot -e "SHOW DATABASES LIKE 'proyecto02';" | grep -q "proyecto02"; then
    echo "🔨 Creando base de datos proyecto02..."
    if ! docker exec mysql mysql -u root -proot -e "CREATE DATABASE proyecto02;"; then
        echo "❌ Error creando base de datos"
        exit 1
    fi
    echo "✅ Base de datos proyecto02 creada"
else
    echo "✅ Base de datos proyecto02 ya existe"
fi

# Verificar si el usuario existe
if ! docker exec mysql mysql -u root -proot -e "SELECT User FROM mysql.user WHERE User='pr';" | grep -q "pr"; then
    echo "🔨 Creando usuario pr..."
    if ! docker exec mysql mysql -u root -proot -e "CREATE USER 'pr'@'%' IDENTIFIED BY 'pr';"; then
        echo "❌ Error creando usuario"
        exit 1
    fi
    echo "✅ Usuario pr creado"
else
    echo "✅ Usuario pr ya existe"
fi

# Asegurar permisos
echo "🔐 Asignando permisos al usuario pr..."
docker exec mysql mysql -u root -proot -e "GRANT ALL PRIVILEGES ON proyecto02.* TO 'pr'@'%';"
docker exec mysql mysql -u root -proot -e "FLUSH PRIVILEGES;"

# Paso 2: Verificar y crear tablas
echo "📊 Paso 2: Configurando tablas..."

# Verificar si las tablas existen
if ! docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;" 2>/dev/null | grep -q "Word"; then
    echo "🔨 Creando tablas..."
    
    # Copiar script SQL al contenedor y ejecutarlo
    docker cp scripts/02_TABLES_PK_COMMENTS.sql mysql:/tmp/
    if ! docker exec mysql mysql -u pr -ppr proyecto02 -e "source /tmp/02_TABLES_PK_COMMENTS.sql"; then
        echo "❌ Error creando tablas"
        exit 1
    fi
    echo "✅ Tablas creadas exitosamente"
else
    echo "✅ Las tablas ya existen"
fi

# Paso 3: Verificación final
echo "🔍 Paso 3: Verificación final..."
docker exec mysql mysql -u pr -ppr -e "USE proyecto02; SHOW TABLES;"

if [ $? -ne 0 ]; then
    echo "❌ Error en la verificación final"
    exit 1
fi

echo "✅ Base de datos configurada correctamente"
echo "📊 Usuario: pr"
echo "📊 Contraseña: pr"
echo "📊 Base de datos: proyecto02"
echo "📊 Puerto: 3307"