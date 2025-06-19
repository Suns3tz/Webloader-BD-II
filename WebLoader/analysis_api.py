import mysql.connector
from mysql.connector import Error
import logging

logger = logging.getLogger(__name__)

class AnalysisAPI:
    def __init__(self):
        self.mysql_config = {
            'host': 'localhost',
            'port': 3307,  # Puerto mapeado en docker-compose
            'user': 'pr',      # Usuario correcto
            'password': 'pr',  # Contraseña correcta
            'database': 'proyecto02'
        }
    
    def get_connection(self):
        """Crear conexión a MySQL"""
        try:
            connection = mysql.connector.connect(**self.mysql_config)
            return connection
        except Error as e:
            logger.error(f"Error conectando a MySQL: {e}")
            return None
    
    def get_top_words_per_page(self, limit=10):
        """Obtener las palabras más frecuentes por página"""
        try:
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    w.word,
                    p.title as page_title,
                    p.url as page_url,
                    twp.quantity,
                    w.total_repetitions as word_total_repetitions
                FROM TopWordPages twp
                JOIN Word w ON twp.id_word = w.id_word
                JOIN Page p ON twp.id_page = p.id_page
                ORDER BY twp.quantity DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return results
            
        except Error as e:
            logger.error(f"Error obteniendo palabras por página: {e}")
            return None
    
    def get_top_word_pairs_per_page(self, limit=10):
        """Obtener los pares de palabras más frecuentes por página"""
        try:
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    CONCAT(sw2.word1, ' ', sw2.word2) as word_pair,
                    p.title as page_title,
                    p.url as page_url,
                    t2wp.repetition_count
                FROM Top2WordsPages t2wp
                JOIN SetWords2 sw2 ON t2wp.id_set2 = sw2.id_set2
                JOIN Page p ON t2wp.id_page = p.id_page
                ORDER BY t2wp.repetition_count DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return results
            
        except Error as e:
            logger.error(f"Error obteniendo pares de palabras: {e}")
            return None
    
    def get_top_word_triplets_per_page(self, limit=10):
        """Obtener las tripletas de palabras más frecuentes por página"""
        try:
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    CONCAT(sw3.word1, ' ', sw3.word2, ' ', sw3.word3) as word_triplet,
                    p.title as page_title,
                    p.url as page_url,
                    t3wp.repetition_count
                FROM Top3WordsPages t3wp
                JOIN SetWords3 sw3 ON t3wp.id_set3 = sw3.id_set3
                JOIN Page p ON t3wp.id_page = p.id_page
                ORDER BY t3wp.repetition_count DESC
                LIMIT %s
            """
            
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return results
            
        except Error as e:
            logger.error(f"Error obteniendo tripletas de palabras: {e}")
            return None
    
    def get_analysis_summary(self):
        """Obtener resumen del análisis"""
        try:
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor(dictionary=True)
            
            # Contar totales
            queries = {
                'total_pages': "SELECT COUNT(*) as count FROM Page",
                'total_words': "SELECT COUNT(*) as count FROM Word",
                'total_word_pairs': "SELECT COUNT(*) as count FROM SetWords2",
                'total_word_triplets': "SELECT COUNT(*) as count FROM SetWords3",
            }
            
            summary = {}
            for key, query in queries.items():
                cursor.execute(query)
                result = cursor.fetchone()
                summary[key] = result['count'] if result else 0
            
            cursor.close()
            connection.close()
            
            return summary
            
        except Error as e:
            logger.error(f"Error obteniendo resumen: {e}")
            return None