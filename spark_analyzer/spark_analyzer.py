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
                "DELETE FROM Top3WordsPages",
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
    
    def save_to_mysql_with_upsert(self, df, table_name):
        """Guardar DataFrame en MySQL usando INSERT ON DUPLICATE KEY UPDATE"""
        try:
            mysql_url = f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
            
            properties = {
                "user": self.mysql_config['user'],
                "password": self.mysql_config['password'],
                "driver": "com.mysql.cj.jdbc.Driver",
                "batchsize": "1000",
                "rewriteBatchedStatements": "true"
            }
            
            # Para la tabla Word, usar INSERT ON DUPLICATE KEY UPDATE
            if table_name == "Word":
                properties["createTableOptions"] = "ENGINE=InnoDB"
                # Usar una estrategia personalizada para manejar duplicados
                df.write \
                    .mode("append") \
                    .option("truncate", "false") \
                    .option("createTableOptions", "ENGINE=InnoDB") \
                    .jdbc(url=mysql_url, table=table_name, properties=properties)
            else:
                df.write \
                    .mode("append") \
                    .jdbc(url=mysql_url, table=table_name, properties=properties)
            
            logger.info(f"✅ Registros guardados en tabla {table_name}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error guardando en MySQL tabla {table_name}: {e}")
            return False
    
    def save_to_mysql_with_ignore(self, df, table_name, batch_size=1000):
        """Guardar DataFrame en MySQL usando INSERT IGNORE para manejar duplicados"""
        try:
            import mysql.connector
            from mysql.connector import Error

            connection = mysql.connector.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.mysql_config['database']
            )
            cursor = connection.cursor()
            rows_list = [row.asDict() for row in df.collect()]

            if table_name == "TopWordPages":
                insert_query = """
                INSERT IGNORE INTO TopWordPages (id_word, id_page, quantity) 
                VALUES (%s, %s, %s)
                """
                data_tuples = [(row['id_word'], row['id_page'], row['quantity']) for row in rows_list]
                
            elif table_name == "Top2WordsPages":
                # Insertar relación Top2WordsPages usando INSERT IGNORE
                insert_query = """
                INSERT IGNORE INTO Top2WordsPages (id_set2, id_page, repetition_count) 
                VALUES (%s, %s, %s)
                """
                data_tuples = [(row['id_set2'], row['id_page'], row['repetition_count']) for row in rows_list]
                
            elif table_name == "Top3WordsPages":
                # Insertar relación Top3WordsPages usando INSERT IGNORE
                insert_query = """
                INSERT IGNORE INTO Top3WordsPages (id_set3, id_page, repetition_count) 
                VALUES (%s, %s, %s)
                """
                data_tuples = [(row['id_set3'], row['id_page'], row['repetition_count']) for row in rows_list]
                
            elif table_name == "SetWords2":
                # Insertar bigramas usando INSERT IGNORE
                insert_query = """
                INSERT IGNORE INTO SetWords2 (word1, word2) 
                VALUES (%s, %s)
                """
                data_tuples = [(row['word1'], row['word2']) for row in rows_list]
                
            elif table_name == "SetWords3":
                # Insertar trigramas usando INSERT IGNORE
                insert_query = """
                INSERT IGNORE INTO SetWords3 (word1, word2, word3) 
                VALUES (%s, %s, %s)
                """
                data_tuples = [(row['word1'], row['word2'], row['word3']) for row in rows_list]
                
            else:
                logger.error(f"❌ Tabla {table_name} no soportada en save_to_mysql_with_ignore")
                return False
            
            # Inserta en lotes pequeños
            for i in range(0, len(data_tuples), batch_size):
                batch = data_tuples[i:i+batch_size]
                cursor.executemany(insert_query, batch)
                connection.commit()

            logger.info(f"✅ {len(data_tuples)} registros procesados en tabla {table_name} (duplicados ignorados)")
            cursor.close()
            connection.close()
            return True

        except Error as e:
            logger.error(f"❌ Error en MySQL para tabla {table_name}: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error guardando en tabla {table_name}: {e}")
            return False
    
    def analyze_word_frequency(self, df):
        """Análisis completo de frecuencia de palabras con manejo robusto de duplicados"""
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
            ).distinct()
            
            # Guardar palabras usando INSERT IGNORE
            if not self.save_words_safely(word_totals):
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
                ).distinct().coalesce(1)
            
            # Guardar relación TopWordPages usando INSERT IGNORE
            if not self.save_to_mysql_with_ignore(top_word_pages, "TopWordPages"):
                return False
            
            logger.info("✅ Análisis de frecuencia de palabras completado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en análisis de palabras: {e}")
            return False
    
    def save_words_safely(self, word_df):
        """Guardar palabras usando INSERT IGNORE con manejo manual"""
        try:
            import mysql.connector
            from mysql.connector import Error
            
            # Obtener conexión directa a MySQL
            connection = mysql.connector.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.mysql_config['database']
            )
            
            cursor = connection.cursor()
            
            # Recopilar todas las palabras
            words_list = [row.asDict() for row in word_df.collect()]
            
            # Insertar usando INSERT IGNORE
            insert_query = """
            INSERT IGNORE INTO Word (word, total_repetitions) 
            VALUES (%s, %s)
            """
            
            # Preparar datos para inserción
            words_data = [(word['word'], word['total_repetitions']) for word in words_list]
            
            # Ejecutar inserción por lotes
            cursor.executemany(insert_query, words_data)
            connection.commit()
            
            logger.info(f"✅ {len(words_data)} palabras procesadas (duplicados ignorados)")
            
            cursor.close()
            connection.close()
            return True
            
        except Error as e:
            logger.error(f"❌ Error en MySQL: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error guardando palabras: {e}")
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
            unique_bigrams = bigram_freq.select("word1", "word2").distinct().coalesce(1)
            
            # Guardar sets de 2 palabras con manejo de duplicados
            if not self.save_to_mysql_with_ignore(unique_bigrams, "SetWords2"):
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
                ).distinct().coalesce(1)
            
            # Guardar relación Top2WordsPages
            if not self.save_to_mysql_with_ignore(top_bigram_pages, "Top2WordsPages"):
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
            unique_trigrams = trigram_freq.select("word1", "word2", "word3").distinct().coalesce(1)
            
            # Guardar sets de 3 palabras con manejo de duplicados
            if not self.save_to_mysql_with_ignore(unique_trigrams, "SetWords3"):
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
                ).distinct().coalesce(1)
            
            # Guardar relación Top3WordsPages
            if not self.save_to_mysql_with_ignore(top_trigram_pages, "Top3WordsPages"):
                return False
            
            logger.info("✅ Análisis de trigramas completado")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en análisis de trigramas: {e}")
            return False
    

    def analyze_TOP10Pages_by_shared_bigrams(self, df):
        try:
            logger.info("🔍 Analizando páginas TOP10 por bigramas compartidos...")

            # Validación inicial
            if df is None or df.rdd.isEmpty():
                logger.warning("⚠️ DataFrame vacío o nulo.")
                return False

            # Filtrar filas con bigrams y URL válidas
            df_filtered = df.filter(col("url").isNotNull() & col("bigrams").isNotNull())

            # Explota los bigramas
            df_exploded = df_filtered.select(
                col("url"),
                explode(col("bigrams")).alias("bigram_str")
            ).filter(col("bigram_str").isNotNull() & (length(col("bigram_str")) > 0))

            if df_exploded.rdd.isEmpty():
                logger.warning("⚠️ No hay bigramas válidos para analizar.")
                return True

            # Evita combinaciones duplicadas (A-B y B-A)
            joined = df_exploded.alias("a").join(
                df_exploded.alias("b"),
                (col("a.bigram_str") == col("b.bigram_str")) & (col("a.url") < col("b.url"))
            ).select(
                col("a.url").alias("page1_url"),
                col("b.url").alias("page2_url"),
                col("a.bigram_str")
            ).distinct()

            # Agrupar por pares y contar coincidencias
            shared_counts = joined.groupBy("page1_url", "page2_url") \
                .agg(count("bigram_str").alias("shared_sets_count"))

            # Top 10 por página
            window = Window.partitionBy("page1_url").orderBy(desc("shared_sets_count"))
            top10 = shared_counts.withColumn("rank", row_number().over(window)) \
                .filter(col("rank") <= 10)

            # Cargar IDs desde MySQL
            page_df = self.spark.read.jdbc(
                url=self.mysql_url, table="Page", properties=self.mysql_properties
            ).select("id_page", "url")

            top10_ids = top10 \
                .join(page_df.withColumnRenamed("id_page", "id_page1").withColumnRenamed("url", "page1_url"), on="page1_url") \
                .join(page_df.withColumnRenamed("id_page", "id_page2").withColumnRenamed("url", "page2_url"), on="page2_url") \
                .select("id_page1", "id_page2", "shared_sets_count")

            if not self.save_to_mysql(top10_ids, "Sets2PageXPage", mode="append"):
                return False

            logger.info("✅ Análisis de TOP10 páginas por bigramas compartidos completado")
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

    def analyze_word_percentage_per_page(self, df):
        "Calcula el porcentaje de cada palabra en el texto total de la página y guarda en PageXWord"
        try:
            logger.info("📈 Calculando porcentaje de palabras por página...")

            # Explotar palabras y contar por página
            word_page_df = df.select(
                col("title").alias("page_title"),
                col("url").alias("page_url"),
                explode(coalesce(col("word_list"), array())).alias("word")
            ).filter(col("word").isNotNull() & (col("word") != ""))

            # Total de palabras por página
            total_words_per_page = word_page_df.groupBy("page_title", "page_url").count().withColumnRenamed("count", "total_words")

            # Frecuencia de cada palabra por página
            word_freq = word_page_df.groupBy("word", "page_title", "page_url").count().withColumnRenamed("count", "quantity")

            # Unir para calcular porcentaje
            joined = word_freq.join(
                total_words_per_page,
                on=["page_title", "page_url"]
            ).withColumn(
                "percentage", col("quantity") / col("total_words")
            )

            # Leer IDs desde MySQL
            pages_mysql = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Page") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()
            words_mysql = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Word") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            # Relacionar con IDs
            result = joined.alias("j") \
                .join(pages_mysql.alias("p"), col("j.page_title") == col("p.title")) \
                .join(words_mysql.alias("w"), col("j.word") == col("w.word")) \
                .select(
                    col("p.id_page"),
                    col("w.id_word"),
                    col("j.percentage"),
                    col("j.quantity")
                )

            # Guardar en PageXWord
            if not self.save_to_mysql(result, "PageXWord", mode="append"):
                return False

            logger.info("✅ Porcentaje de palabras por página calculado y guardado")
            return True

        except Exception as e:
            logger.error(f"❌ Error en porcentaje de palabras: {e}")
            return False

    def analyze_word_frequency_in_links(self, df):
        """Cuenta cuántas veces se repite cada palabra en los textos de los links de todas las páginas y guarda en Word.total_repetitions"""
        try:
            logger.info("🔗 Analizando frecuencia de palabras en los textos de los links de todas las páginas...")

            # Obtener todas las páginas enlazadas
            page_links_df = df.select(
                explode(coalesce(col("links"), array())).alias("linked_url")
            )

            # Unir para obtener el word_list de cada página enlazada
            links_words_df = page_links_df.join(
                df.select(
                    col("url").alias("linked_url"),
                    col("word_list")
                ),
                on="linked_url",
                how="inner"
            )

            # Explotar palabras de los links
            words_in_links = links_words_df.select(
                explode(coalesce(col("word_list"), array())).alias("word")
            ).filter(col("word").isNotNull() & (col("word") != ""))

            # Contar frecuencia global de cada palabra en los links
            word_counts = words_in_links.groupBy("word").count().withColumnRenamed("count", "total_repetitions")

            # Guardar en Word (sobrescribe para mantener actualizado)
            if not self.save_words_safely(word_counts):
                return False

            logger.info("✅ Frecuencia de palabras en links calculada y guardada en Word.total_repetitions")
            return True

        except Exception as e:
            logger.error(f"❌ Error en frecuencia de palabras en links: {e}")
            return False

    def analyze_repeated_links(self, df):
        """Cuenta cuántas veces se repite cada link en todos los links de todas las páginas y guarda en Page.total_repetitions"""
        try:
            logger.info("🔗 Analizando links repetidos en todas las páginas...")

            # Explotar todos los links
            all_links = df.select(
                explode(coalesce(col("links"), array())).alias("link_url")
            ).filter(col("link_url").isNotNull() & (col("link_url") != ""))

            # Contar repeticiones de cada link
            link_counts = all_links.groupBy("link_url").count().withColumnRenamed("count", "total_repetitions")

            # Leer tabla Page
            pages_mysql = self.spark.read.format("jdbc") \
                .option("url", f"jdbc:mysql://{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}") \
                .option("dbtable", "Page") \
                .option("user", self.mysql_config['user']) \
                .option("password", self.mysql_config['password']) \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .load()

            # Unir para actualizar el campo total_repetitions de cada página (por url)
            updated_pages = pages_mysql.join(
                link_counts, pages_mysql.url == link_counts.link_url, "left"
            ).select(
                pages_mysql["*"],
                link_counts["total_repetitions"]
            )

            # Guardar en Page (actualiza solo el campo total_repetitions)
            if not self.update_page_total_repetitions(updated_pages):
                return False

            logger.info("✅ Links repetidos contados y guardados en Page.total_repetitions")
            return True

        except Exception as e:
            logger.error(f"❌ Error en links repetidos: {e}")
            return False

    def update_page_total_repetitions(self, updated_pages_df):
        """Actualiza el campo total_repetitions de la tabla Page de forma segura (por url)"""
        try:
            import mysql.connector
            from mysql.connector import Error

            # Recopilar los datos a actualizar
            rows_list = [row.asDict() for row in updated_pages_df.collect()]

            # Conectar a MySQL
            connection = mysql.connector.connect(
                host=self.mysql_config['host'],
                port=self.mysql_config['port'],
                user=self.mysql_config['user'],
                password=self.mysql_config['password'],
                database=self.mysql_config['database']
            )
            cursor = connection.cursor()

            # Preparar y ejecutar UPDATE para cada página
            update_query = """
            UPDATE Page SET total_repetitions = %s WHERE url = %s
            """
            data_tuples = []
            for row in rows_list:
                # Si no hay valor, poner 0
                total_repetitions = row.get('total_repetitions')
                if total_repetitions is None:
                    total_repetitions = 0
                data_tuples.append((total_repetitions, row['url']))
            if data_tuples:
                cursor.executemany(update_query, data_tuples)
                connection.commit()
                logger.info(f"✅ {len(data_tuples)} páginas actualizadas (total_repetitions)")
            cursor.close()
            connection.close()
            return True
        except Error as e:
            logger.error(f"❌ Error en MySQL al actualizar Page: {e}")
            return False
        except Exception as e:
            logger.error(f"❌ Error actualizando Page: {e}")
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
            
            # # 2. Analizar palabras
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
            #if not self.analyze_TOP10Pages_by_shared_bigrams(df):
                #logger.error("❌ Falló el análisis de TOP10 páginas por bigramas compartidos")
                #return False    
            
            # 6. Analizar TOP10 páginas por trigramas compartidos
            #if not self.analyze_TOP10Pages_by_shared_trigrams(df):
                #logger.error("❌ Falló el análisis de TOP10 páginas por trigramas compartidos")
                #return False
            
            # 7. Análisis de palabras por página
            #if not self.ForEach_Page_Words(df):
                #logger.error("❌ Falló el análisis de palabras por página")
                #return False
            
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