# WebLoader - Sistema de Análisis de Páginas Web

## 🚀 Mejoras Implementadas

### ✅ Correcciones Realizadas

1. **API de Funciones SQL Corregida**
   - Manejo mejorado de errores en `analysis_api.py`
   - Respuestas JSON consistentes con códigos de estado HTTP apropiados
   - Validación de parámetros y conexiones de base de datos

2. **Frontend Actualizado**
   - Nueva sección "Explorador de Funciones SQL" para probar todas las funciones de la base de datos
   - Interfaz intuitiva con formularios específicos para cada tipo de consulta
   - Botones de ayuda para mostrar páginas y palabras disponibles
   - Eliminación de funciones obsoletas y código duplicado

3. **Funciones SQL Disponibles**
   - 🔍 **Buscar por palabra**: Encuentra páginas que más contienen una palabra específica
   - 🔗 **Buscar por par de palabras**: Páginas con conjuntos de 2 palabras
   - 🎯 **Buscar por tripleta**: Páginas con conjuntos de 3 palabras
   - 📄 **Análisis por URL**: 6 tipos de análisis diferentes por página
   - 📊 **Repeticiones de palabra**: Total de ocurrencias en toda la base de datos

### 🔧 Funcionalidades Nuevas

#### Explorador de Funciones SQL
- **Búsqueda por Palabra Individual**: Utiliza `getTopPagesByWord(palabra)`
- **Búsqueda por Par de Palabras**: Utiliza `getTopPagesBySet2(palabra1, palabra2)`
- **Búsqueda por Tripleta**: Utiliza `getTopPagesBySet3(palabra1, palabra2, palabra3)`
- **Análisis por URL**:
  - Palabras distintas: `getDifferentWordsByPage(url)`
  - Conteo de links: `getLinkCountByPage(url)`
  - Porcentajes: `getPercentageWordsByPage(url)`
  - Bigramas compartidos: `getSharedBigramsByPage(url)`
  - Trigramas compartidos: `getSharedTrigramsByPage(url)`
  - Total repeticiones: `getTotalRepetitionsByPage(url)`
- **Repeticiones Totales**: `getTotalRepetitionsByWord(palabra)`

#### Funciones de Ayuda
- **Ver Páginas Disponibles**: Muestra las primeras 20 páginas en la base de datos
- **Ver Palabras Populares**: Muestra las 50 palabras más repetidas
- **Autocompletado**: Clic en páginas/palabras las copia automáticamente a los campos

### 🛠️ Mejoras Técnicas

1. **Manejo de Errores Robusto**
   ```python
   # Antes: Retornaba None en errores
   if results is None:
       return jsonify({'error': 'Error genérico'}), 500
   
   # Ahora: Manejo específico de errores
   if isinstance(results, dict) and 'error' in results:
       return jsonify(results), 500  # Error específico de la función SQL
   ```

2. **Respuestas API Consistentes**
   ```json
   {
     "success": true,
     "data": [...] // Datos reales de la función SQL
   }
   ```

3. **Código Limpio**
   - Eliminación de funciones deprecated
   - Reducción de duplicación de código
   - Mejor organización de componentes

### 📱 Interfaz Mejorada

#### Antes
- Solo formulario genérico de análisis
- Resultados limitados y confusos
- Sin forma fácil de probar funciones específicas

#### Ahora
- Sección dedicada para explorar funciones SQL
- Campos específicos para cada tipo de consulta
- Botones de ayuda para facilitar las pruebas
- Resultados claros en tablas organizadas
- Mensajes de error específicos y útiles

### 🔍 Cómo Usar las Nuevas Funciones

1. **Inicia la aplicación**:
   ```bash
   cd WebLoader
   python app.py
   ```

2. **Abre http://localhost:5000** en tu navegador

3. **Usa los botones de ayuda**:
   - "Ver Páginas Disponibles" para obtener URLs reales
   - "Ver Palabras Populares" para obtener palabras que existen en la BD

4. **Prueba las funciones**:
   - Completa los campos de búsqueda
   - Los resultados aparecen en tablas organizadas
   - Los errores se muestran de forma clara

### 🗃️ Estructura API

#### Endpoints de Funciones SQL
- `GET /api/analysis/word/<palabra>` - Páginas por palabra
- `GET /api/analysis/word-set2/<palabra1>/<palabra2>` - Páginas por par
- `GET /api/analysis/word-set3/<p1>/<p2>/<p3>` - Páginas por tripleta
- `GET /api/analysis/shared-bigrams?url=<url>` - Bigramas compartidos
- `GET /api/analysis/shared-trigrams?url=<url>` - Trigramas compartidos
- `GET /api/analysis/page-words?url=<url>` - Palabras de página
- `GET /api/analysis/page-links?url=<url>` - Links de página
- `GET /api/analysis/page-word-percentages?url=<url>` - Porcentajes
- `GET /api/analysis/word-repetitions/<palabra>` - Total repeticiones
- `GET /api/analysis/page-repetitions?url=<url>` - Repeticiones de página

#### Endpoints de Ayuda
- `GET /api/helper/pages?limit=N` - Páginas disponibles
- `GET /api/helper/words?limit=N` - Palabras populares

### 🎯 Resultados

✅ **Error de base de datos corregido**: Manejo robusto de errores SQL
✅ **Funciones SQL accesibles**: Todas las 11 funciones disponibles vía API
✅ **Frontend mejorado**: Interfaz intuitiva para probar funciones
✅ **Código limpio**: Eliminación de duplicaciones y funciones obsoletas
✅ **Experiencia de usuario**: Botones de ayuda y autocompletado
✅ **Documentación**: API bien documentada y estructurada

### 🚦 Pruebas Recomendadas

1. **Conectividad de Base de Datos**: Verifica el estado en la interfaz
2. **Funciones por Palabra**: Prueba con palabras del botón de ayuda
3. **Funciones por URL**: Usa URLs del botón "Ver Páginas Disponibles"
4. **Manejo de Errores**: Prueba con datos inexistentes
5. **Respuesta de API**: Verifica que los JSON son válidos y consistentes

La aplicación ahora está completamente funcional con todas las funciones SQL accesibles a través de una interfaz web moderna y amigable.

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

