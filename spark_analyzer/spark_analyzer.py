from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql.window import Window


import json
import sys
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WikiDataAnalyzer:
    def __init__(self):
        self.spark = None
        self.mysql_config = {
            'host': 'mysql',
            'port': 3306,
            'user': 'pr',
            'password': 'pr',
            'database': 'proyecto02'
        }
        self.mysql_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        self.mysql_properties = {
            "user": self.mysql_config['user'],
            "password": self.mysql_config['password'],
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def init_spark(self):
        """Inicializar sesión de Spark"""
        try:
            self.spark = SparkSession.builder \
                .appName("WikiDataAnalyzer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("WARN")
            logger.info("✅ Spark Session inicializada correctamente")
            return True
        except Exception as e:
            logger.error(f"❌ Error inicializando Spark: {e}")
            return False
    
    def clear_tables(self):
        """Limpiar todas las tablas respetando claves foráneas"""
        try:
            logger.info("🧹 Limpiando tablas MySQL...")
            
            mysql_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
            properties = {
                "user": self.mysql_config['user'],
                "password": self.mysql_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            # Crear conexión temporal para ejecutar DELETE
            temp_df = self.spark.createDataFrame([(1,)], ["dummy"])
            
            # Orden correcto para eliminar (respetando claves foráneas)
            tables_to_clear = [
                "TopWordPages",
                "Top2WordsPages", 
                "Top3WordsPages",
                "PageXWord",
                "Sets2PageXPage",
                "Sets3PageXPage",
                "SetWords2",
                "SetWords3", 
                "Word",
                "Page"
            ]
            
            for table in tables_to_clear:
                try:
                    # Usar TRUNCATE que es más rápido y respeta AUTO_INCREMENT
                    query = f"(SELECT 1 as dummy) t"
                    temp_df.write \
                        .mode("append") \
                        .option("truncate", "true") \
                        .option("createTableOptions", f"ENGINE=InnoDB") \
                        .option("beforeCommit", f"TRUNCATE TABLE {table}") \
                        .jdbc(url=mysql_url, table=table, properties=properties)
                    logger.info(f"✅ Tabla {table} limpiada")
                except Exception as e:
                    logger.warning(f"⚠️ No se pudo limpiar tabla {table}: {e}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error limpiando tablas: {e}")
            return False
    
    def execute_sql_directly(self, sql_command):
        """Ejecutar comando SQL directamente"""
        try:
            mysql_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
            properties = {
                "user": self.mysql_config['user'],
                "password": self.mysql_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            # Crear un DataFrame dummy para ejecutar el comando
            dummy_df = self.spark.createDataFrame([(1,)], ["dummy"])
            
            # Usar beforeCommit para ejecutar el SQL
            dummy_df.write \
                .mode("append") \
                .option("createTableOptions", "ENGINE=InnoDB") \
                .option("beforeCommit", sql_command) \
                .jdbc(url=mysql_url, table="dummy_table", properties=properties)
            
            return True
        except Exception as e:
            logger.error(f"Error ejecutando SQL: {e}")
            return False
    
    def clear_tables_with_sql(self):
        """Limpiar tablas usando SQL directo"""
        try:
            logger.info("🧹 Limpiando tablas con SQL directo...")
            
            # Orden correcto para eliminar datos
            clear_commands = [
                "DELETE FROM TopWordPages",
                "DELETE FROM Top2WordsPages", 
                "DELETE FROM Top3WordPages",
                "DELETE FROM PageXWord",
                "DELETE FROM Sets2PageXPage",
                "DELETE FROM Sets3PageXPage",
                "DELETE FROM SetWords2",
                "DELETE FROM SetWords3", 
                "DELETE FROM Word",
                "DELETE FROM Page"
            ]
            
            # Ejecutar todos los comandos en una sola transacción
            all_commands = "; ".join(clear_commands)
            
            if self.execute_sql_directly(all_commands):
                logger.info("✅ Todas las tablas limpiadas correctamente")
                return True
            else:
                logger.error("❌ Error limpiando tablas")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en limpieza de tablas: {e}")
            return False
    
    def load_data_from_hdfs(self, hdfs_path="hdfs://namenode:9000/user/root/wiki_data/wiki_data.jsonl"):
        """Cargar datos desde HDFS"""
        try:
            logger.info(f"📂 Intentando cargar datos desde: {hdfs_path}")
            
            # Leer archivo JSONL desde HDFS
            df = self.spark.read.text(hdfs_path)
            
            # Definir schema para el JSON
            schema = StructType([
                StructField("url", StringType(), True),
                StructField("title", StringType(), True),
                StructField("word_list", ArrayType(StringType()), True),
                StructField("bigrams", ArrayType(StringType()), True),
                StructField("trigrams", ArrayType(StringType()), True),
                StructField("edits_per_day", DoubleType(), True),
                StructField("links", ArrayType(StringType()), True)
            ])
            
            # Parsear JSON y extraer campos
            parsed_df = df.select(
                from_json(col("value"), schema).alias("data")
            ).select("data.*")
            
            # Filtrar registros nulos
            parsed_df = parsed_df.filter(col("title").isNotNull())
            
            row_count = parsed_df.count()
            logger.info(f"✅ Datos cargados desde HDFS: {row_count} registros")
            
            if row_count == 0:
                logger.error("❌ No se encontraron datos válidos")
                return None
                
            return parsed_df
            
        except Exception as e:
            logger.error(f"❌ Error cargando datos desde HDFS: {e}")
            return None
    
    def save_to_mysql(self, df, table_name, mode="append"):
        """Guardar DataFrame en MySQL usando Spark JDBC"""
        try:
            mysql_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
            
            properties = {
                "user": self.mysql_config['user'],
                "password": self.mysql_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver"
            }
            
            # Escribir datos a MySQL
            df.write \
                .mode(mode) \
                .jdbc(url=mysql_url, table=table_name, properties=properties)
            
            logger.info(f"✅ {df.count()} registros guardados en tabla {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando en MySQL tabla {table_name}: {e}")
            return False
    
    def analyze_and_save_pages(self, df):
        """Analizar y guardar páginas"""
        try:
            logger.info("📄 Analizando páginas...")
            
            # Preparar datos de páginas
            pages_df = df.select(
                col("title"),
                col("url"),
                coalesce(col("edits_per_day"), lit(0.0)).cast("double").alias("edits_per_day"),
                size(coalesce(col("links"), array())).alias("quant_diff_urls"),
                size(coalesce(col("bigrams"), array())).alias("quant_set2"),
                size(coalesce(col("trigrams"), array())).alias("quant_set3"),
                lit(1).alias("total_repetitions")
            )
            
            # Guardar en MySQL (modo append ya que limpiamos antes)
            if self.save_to_mysql(pages_df, "Page", mode="append"):
                logger.info(f"✅ {pages_df.count()} páginas guardadas")
                return True
            return False
            
        except Exception as e:
            logger.error(f"❌ Error analizando páginas: {e}")
            return False
    
    def analyze_word_frequency(self, df):
        """Análisis completo de frecuencia de palabras"""
        try:
            logger.info("🔤 Analizando frecuencia de palabras...")
            
            # Explotar palabras y contar por página
            word_page_df = df.select(
                col("title").alias("page_title"),
                col("url").alias("page_url"),
                explode(coalesce(col("word_list"), array())).alias("word")
            ).filter(col("word").isNotNull() & (col("word") != ""))
            
            if word_page_df.count() == 0:
                logger.warning("⚠️ No se encontraron palabras para analizar")
                return True
            
            # Contar frecuencias por palabra y página
            word_freq = word_page_df.groupBy("word", "page_title", "page_url") \
                .count() \
                .withColumnRenamed("count", "frequency")
            
            # Calcular total de repeticiones por palabra
            word_totals = word_freq.groupBy("word").agg(
                sum("frequency").alias("total_repetitions")
            )
            
            # Guardar palabras únicas
            if not self.save_to_mysql(word_totals, "Word", mode="append"):
                return False
            
            # Leer IDs de las páginas desde MySQL
            pages_mysql = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Page") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # Leer IDs de las palabras desde MySQL
            words_mysql = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Word") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # Crear TopWordPages con IDs correctos
            top_word_pages = word_freq.alias("wf") \
                .join(pages_mysql.alias("p"), col("wf.page_title") == col("p.title")) \
                .join(words_mysql.alias("w"), col("wf.word") == col("w.word")) \
                .select(
                    col("w.id_word"),
                    col("p.id_page"),
                    col("wf.frequency").alias("quantity")
                )
            
            # Guardar relación TopWordPages
            if not self.save_to_mysql(top_word_pages, "TopWordPages", mode="append"):
                return False
            
            logger.info("✅ Análisis de frecuencia de palabras completado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en análisis de palabras: {e}")
            return False
    
    def analyze_bigrams(self, df):
        """Análisis completo de pares de palabras"""
        try:
            logger.info("🔗 Analizando pares de palabras...")
            
            # Explotar bigramas
            bigram_page_df = df.select(
                col("title").alias("page_title"),
                col("url").alias("page_url"),
                explode(coalesce(col("bigrams"), array())).alias("bigram")
            ).filter(col("bigram").isNotNull() & (col("bigram") != ""))
            
            if bigram_page_df.count() == 0:
                logger.warning("⚠️ No se encontraron bigramas para analizar")
                return True
            
            # Separar el bigrama en dos palabras
            bigram_split_df = bigram_page_df.withColumn(
                "words_split", split(col("bigram"), " ")
            ).filter(size(col("words_split")) >= 2).select(
                col("page_title"),
                col("page_url"),
                col("bigram"),
                col("words_split")[0].alias("word1"),
                col("words_split")[1].alias("word2")
            ).filter(
                col("word1").isNotNull() & 
                col("word2").isNotNull() & 
                (col("word1") != "") & 
                (col("word2") != "")
            )
            
            # Contar frecuencias de bigramas por página
            bigram_freq = bigram_split_df.groupBy("word1", "word2", "page_title", "page_url") \
                .count() \
                .withColumnRenamed("count", "repetition_count")
            
            # Obtener sets únicos de 2 palabras
            unique_bigrams = bigram_freq.select("word1", "word2").distinct()
            
            # Guardar sets de 2 palabras
            if not self.save_to_mysql(unique_bigrams, "SetWords2", mode="append"):
                return False
            
            # Leer datos desde MySQL para obtener IDs
            pages_mysql = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Page") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            setwords2_mysql = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "SetWords2") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # Crear Top2WordsPages con IDs correctos
            top_bigram_pages = bigram_freq.alias("bf") \
                .join(pages_mysql.alias("p"), col("bf.page_title") == col("p.title")) \
                .join(setwords2_mysql.alias("sw2"), 
                      (col("bf.word1") == col("sw2.word1")) & (col("bf.word2") == col("sw2.word2"))) \
                .select(
                    col("sw2.id_set2"),
                    col("p.id_page"),
                    col("bf.repetition_count")
                )
            
            # Guardar relación Top2WordsPages
            if not self.save_to_mysql(top_bigram_pages, "Top2WordsPages", mode="append"):
                return False
            
            logger.info("✅ Análisis de bigramas completado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en análisis de bigramas: {e}")
            return False
    
    def analyze_trigrams(self, df):
        """Análisis completo de tripletas de palabras"""
        try:
            logger.info("🎯 Analizando tripletas de palabras...")
            
            # Explotar trigramas
            trigram_page_df = df.select(
                col("title").alias("page_title"),
                col("url").alias("page_url"),
                explode(coalesce(col("trigrams"), array())).alias("trigram")
            ).filter(col("trigram").isNotNull() & (col("trigram") != ""))
            
            if trigram_page_df.count() == 0:
                logger.warning("⚠️ No se encontraron trigramas para analizar")
                return True
            
            # Separar el trigrama en tres palabras
            trigram_split_df = trigram_page_df.withColumn(
                "words_split", split(col("trigram"), " ")
            ).filter(size(col("words_split")) >= 3).select(
                col("page_title"),
                col("page_url"),
                col("trigram"),
                col("words_split")[0].alias("word1"),
                col("words_split")[1].alias("word2"),
                col("words_split")[2].alias("word3")
            ).filter(
                col("word1").isNotNull() & 
                col("word2").isNotNull() & 
                col("word3").isNotNull() & 
                (col("word1") != "") & 
                (col("word2") != "") & 
                (col("word3") != "")
            )
            
            # Contar frecuencias de trigramas por página
            trigram_freq = trigram_split_df.groupBy("word1", "word2", "word3", "page_title", "page_url") \
                .count() \
                .withColumnRenamed("count", "repetition_count")
            
            # Obtener sets únicos de 3 palabras
            unique_trigrams = trigram_freq.select("word1", "word2", "word3").distinct()
            
            # Guardar sets de 3 palabras
            if not self.save_to_mysql(unique_trigrams, "SetWords3", mode="append"):
                return False
            
            # Leer datos desde MySQL
            pages_mysql = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Page") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            setwords3_mysql = self.spark.read \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "SetWords3") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            
            # Crear Top3WordsPages con IDs correctos
            top_trigram_pages = trigram_freq.alias("tf") \
                .join(pages_mysql.alias("p"), col("tf.page_title") == col("p.title")) \
                .join(setwords3_mysql.alias("sw3"), 
                      (col("tf.word1") == col("sw3.word1")) & 
                      (col("tf.word2") == col("sw3.word2")) & 
                      (col("tf.word3") == col("sw3.word3"))) \
                .select(
                    col("sw3.id_set3"),
                    col("p.id_page"),
                    col("tf.repetition_count")
                )
            
            # Guardar relación Top3WordsPages
            if not self.save_to_mysql(top_trigram_pages, "Top3WordsPages", mode="append"):
                return False
            
            logger.info("✅ Análisis de trigramas completado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en análisis de trigramas: {e}")
            return False
    

    def analyze_TOP10Pages_by_shared_bigrams(self, df):

        try:
            logger.info("🔍 Analizando páginas TOP10 por bigramas compartidos...")
            # Aquí iría la lógica para analizar las páginas TOP10 por bigramas compartidos
            
            # Explota los bigramas y los convierte a string
            df_exploded = df.select("url", explode(col("bigrams")).alias("bigram_str"))

            # Relaciona páginas que comparten bigramas
            joined = df_exploded.alias("a").join(
                df_exploded.alias("b"),
                (col("a.bigram_str") == col("b.bigram_str")) & (col("a.url") != col("b.url"))
            ).select(
                col("a.url").alias("page1_url"),
                col("b.url").alias("page2_url"),
                col("a.bigram_str")
            ).distinct()

            # Cuenta cuántos bigramas comparten cada par de páginas
            shared_counts = joined.groupBy("page1_url", "page2_url").agg(count("bigram_str").alias("shared_sets_count"))

            # Top 10 de páginas que más bigramas comparten con cada página
            window = Window.partitionBy("page1_url").orderBy(desc("shared_sets_count"))
            top10 = shared_counts.withColumn("rank", row_number().over(window)).filter(col("rank") <= 10)

            page_df = self.spark.read.jdbc(url=self.mysql_url, table="Page", properties=self.mysql_properties).select("id_page", "url")

            top10_ids = top10 \
                .join(page_df.withColumnRenamed("id_page", "id_page1").withColumnRenamed("url", "page1_url"), on="page1_url") \
                .join(page_df.withColumnRenamed("id_page", "id_page2").withColumnRenamed("url", "page2_url"), on="page2_url") \
                .select("id_page1", "id_page2", "shared_sets_count") \

            if not self.save_to_mysql(top10_ids, "Sets2PageXPage", mode="append"):
                return False
            
            return True
        
        except Exception as e:
            logger.error(f"❌ Error en análisis de páginas TOP10 por bigramas compartidos: {e}")
            return False
            
        
        
    def analyze_TOP10Pages_by_shared_trigrams(self, df):

        try:
            logger.info("🔍 Analizando páginas TOP10 por trigramas compartidos...")

            # Explota los trigramas y los convierte a string
            df_exploded = df.select("url", explode(col("trigrams")).alias("trigram_str"))

            # Relaciona páginas que comparten trigramas
            joined = df_exploded.alias("a").join(
                df_exploded.alias("b"),
                (col("a.trigram_str") == col("b.trigram_str")) & (col("a.url") != col("b.url"))
            ).select(
                col("a.url").alias("page1_url"),
                col("b.url").alias("page2_url"),
                col("a.trigram_str")
            ).distinct()

            # Cuenta cuántos trigramas comparten cada par de páginas
            shared_counts = joined.groupBy("page1_url", "page2_url").agg(count("trigram_str").alias("shared_sets_count"))

            # Top 10 de páginas que más trigramas comparten con cada página
            window = Window.partitionBy("page1_url").orderBy(desc("shared_sets_count"))
            top10 = shared_counts.withColumn("rank", row_number().over(window)).filter(col("rank") <= 10)

            page_df = self.spark.read.jdbc(url=self.mysql_url, table="Page", properties=self.mysql_properties).select("id_page", "url")

            top10_ids = top10 \
                .join(page_df.withColumnRenamed("id_page", "id_page1").withColumnRenamed("url", "page1_url"), on="page1_url") \
                .join(page_df.withColumnRenamed("id_page", "id_page2").withColumnRenamed("url", "page2_url"), on="page2_url") \
                .select("id_page1", "id_page2", "shared_sets_count") \

            if not self.save_to_mysql(top10_ids, "Sets3PageXPage", mode="append"):
                return False
            
            return True

        except Exception as e:
            logger.error(f"❌ Error en análisis de páginas TOP10 por trigramas compartidos: {e}")
            return False



    def ForEach_Page_Words(self, df):
        """Realiza un análisis de palabras para cada página"""
        try:
            logger.info("🔍 Analizando palabras por página...")
            
            # Explota la lista de palabras
            exploded = df.select(col("url"), explode(col("word_list")).alias("word"))

            # Cuenta palabras por página
            word_counts = exploded.groupBy("url", "word").agg(count("*").alias("quantity"))

            # Cuenta total de palabras por página
            total_counts = exploded.groupBy("url").agg(count("*").alias("total_words"))

            # Une ambos DataFrames
            joined = word_counts.join(total_counts, on="url")

            # Calcula el porcentaje
            result = joined.withColumn(
                "percentage",
                (col("quantity") / col("total_words")) * 100
            )

            # Carga los IDs de página y palabra desde MySQL
            page_df = self.spark.read.jdbc(
                url=self.mysql_url, table="Page", properties=self.mysql_properties
            ).select("id_page", "url")
            word_df = self.spark.read.jdbc(
                url=self.mysql_url, table="Word", properties=self.mysql_properties
            ).select("id_word", "word")

            # Une para obtener los IDs
            result = result.join(page_df, on="url") \
                        .join(word_df, on="word") \
                        .select("id_page", "id_word", "percentage", "quantity")

            # Guarda la relación PageXWord
            if not self.save_to_mysql(result, "PageXWord", mode="append"):
                return False
            logger.info("✅ Análisis de palabras por página completado")
            return True
        except Exception as e:
            logger.error(f"❌ Error en análisis de palabras por página: {e}")
            return False

    def verify_hdfs_connection(self):
        """Verificar conexión a HDFS"""
        try:
            logger.info("🔍 Verificando conexión a HDFS...")
            # Aquí iría la lógica para verificar la conexión a HDFS

            # Intentar listar el directorio HDFS
            test_df = self.spark.read.text("hdfs://namenode:9000/")
            logger.info("✅ Conexión a HDFS exitosa")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error conectando a HDFS: {e}")
            return False
    
    def run_complete_analysis(self):
        """Ejecutar análisis completo"""
        try:
            # Inicializar Spark
            if not self.init_spark():
                return False
            
            # Verificar conexión a HDFS
            if not self.verify_hdfs_connection():
                logger.error("❌ No se puede conectar a HDFS")
                return False
            
            # Limpiar tablas antes de comenzar
            logger.info("🧹 Limpiando datos anteriores...")
            # Como tenemos claves foráneas, simplemente usamos DELETE en lugar de TRUNCATE/DROP
            
            # Cargar datos
            df = self.load_data_from_hdfs()
            if df is None:
                return False
            
            # Mostrar información de los datos cargados
            logger.info("📊 Información de los datos cargados:")
            logger.info(f"   - Total de registros: {df.count()}")
            
            # Mostrar muestra de datos
            logger.info("📋 Muestra de datos:")
            df.select("title", "url").show(5, truncate=False)
            
            # Ejecutar análisis secuencial
            logger.info("📊 Iniciando análisis completo...")
            
            # 1. Analizar y guardar páginas
            if not self.analyze_and_save_pages(df):
                logger.error("❌ Falló el análisis de páginas")
                return False
            
            # 2. Analizar palabras
            if not self.analyze_word_frequency(df):
                logger.error("❌ Falló el análisis de palabras")
                return False
            
            # 3. Analizar bigramas
            if not self.analyze_bigrams(df):
                logger.error("❌ Falló el análisis de bigramas")
                return False
            
            # 4. Analizar trigramas
            if not self.analyze_trigrams(df):
                logger.error("❌ Falló el análisis de trigramas")
                return False
            
            # 5. Analizar TOP10 páginas por bigramas compartidos
            if not self.analyze_TOP10Pages_by_shared_bigrams(df):
                logger.error("❌ Falló el análisis de TOP10 páginas por bigramas compartidos")
                return False    
            
            # 6. Analizar TOP10 páginas por trigramas compartidos
            if not self.analyze_TOP10Pages_by_shared_trigrams(df):
                logger.error("❌ Falló el análisis de TOP10 páginas por trigramas compartidos")
                return False
            
            # 7. Análisis de palabras por página
            if not self.ForEach_Page_Words(df):
                logger.error("❌ Falló el análisis de palabras por página")
                return False
            

            
            self.spark.stop()
            
            logger.info("🎉 Análisis completo terminado exitosamente")
            logger.info("💾 Todos los datos han sido guardados en MySQL")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en análisis completo: {e}")
            if self.spark:
                self.spark.stop()
            return False

if __name__ == "__main__":
    logger.info("🚀 Iniciando WikiDataAnalyzer...")
    analyzer = WikiDataAnalyzer()
    success = analyzer.run_complete_analysis()
    
    if success:
        logger.info("✅ Proceso completado exitosamente")
    else:
        logger.error("❌ Proceso falló")
    
    sys.exit(0 if success else 1)