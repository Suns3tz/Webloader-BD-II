import mysql.connector
from mysql.connector import Error

import logging
import json

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
    
    def _execute_function(self, function_name, *args):
        """Método genérico para ejecutar funciones SQL y parsear el resultado JSON"""
        try:
            connection = self.get_connection()
            if not connection:
                return {'error': 'No se pudo conectar a la base de datos'}
            
            cursor = connection.cursor()
            
            # Crear la llamada a la función con los argumentos
            placeholders = ', '.join(['%s'] * len(args))
            query = f"SELECT {function_name}({placeholders}) as result"
            
            cursor.execute(query, args)
            result = cursor.fetchone()
            
            cursor.close()
            connection.close()
            
            if result and result[0]:
                try:
                    # Parsear el JSON retornado por la función
                    parsed_result = json.loads(result[0])
                    
                    # Verificar si el resultado es un error
                    if isinstance(parsed_result, dict) and 'error' in parsed_result:
                        return parsed_result
                    
                    # Si no hay datos, retornar un arreglo vacío
                    if parsed_result is None:
                        return []
                    
                    return parsed_result
                    
                except json.JSONDecodeError as e:
                    logger.error(f"Error parseando JSON de función {function_name}: {e}")
                    return {'error': f'Error parseando resultado de {function_name}'}
            
            return []
            
        except Error as e:
            logger.error(f"Error ejecutando función {function_name}: {e}")
            return {'error': f'Error ejecutando función {function_name}: {str(e)}'}
        except Exception as e:
            logger.error(f"Error inesperado en función {function_name}: {e}")
            return {'error': f'Error inesperado: {str(e)}'}
      
    def get_top_pages_by_word(self, word):
        """Obtener las páginas que más copias de una palabra tienen"""
        return self._execute_function('getTopPagesByWord', word)
    
    def get_top_pages_by_word_set2(self, word1, word2):
        """Obtener las páginas que más copias de un set de 2 palabras tienen"""
        return self._execute_function('getTopPagesBySet2', word1, word2)
    
    def get_top_pages_by_word_set3(self, word1, word2, word3):
        """Obtener las páginas que más copias de un set de 3 palabras tienen"""
        return self._execute_function('getTopPagesBySet3', word1, word2, word3)
    
    def get_shared_bigrams_by_page(self, url):
        """Obtener páginas con más sets de 2 palabras coincidentes con una página dada"""
        return self._execute_function('getSharedBigramsByPage', url)
    
    def get_shared_trigrams_by_page(self, url):
        """Obtener páginas con más sets de 3 palabras coincidentes con una página dada"""
        return self._execute_function('getSharedTrigramsByPage', url)
    
    def get_different_words_by_page(self, url):
        """Obtener el set de palabras distintas de una página y su cantidad"""
        return self._execute_function('getDifferentWordsByPage', url)
    
    def get_link_count_by_page(self, url):
        """Obtener la cantidad de links distintos que aparecen en una página"""
        return self._execute_function('getLinkCountByPage', url)
    
    def get_percentage_words_by_page(self, url):
        """Obtener el porcentaje que representa cada palabra en el texto total de la página"""
        return self._execute_function('getPercentageWordsByPage', url)
    
    def get_total_repetitions_by_word(self, word):
        """Obtener la cantidad total de repeticiones de una palabra"""
        return self._execute_function('getTotalRepetitionsByWord', word)
    
    def get_total_repetitions_by_page(self, url):
        """Obtener la cantidad total de repeticiones de una página"""
        return self._execute_function('getTotalRepetitionsByPage', url)
    
    def get_analysis_summary(self):
        """Obtener resumen del análisis"""
        try:
            connection = self.get_connection()
            if not connection:
                return None
            
            cursor = connection.cursor(dictionary=True)
            
            # Contar totales usando consultas simples ya que no hay stored procedures específicas para estos conteos
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
    
    def get_available_pages(self, limit=50):
        """Obtener lista de páginas disponibles para facilitar las pruebas"""
        try:
            connection = self.get_connection()
            if not connection:
                return {'error': 'No se pudo conectar a la base de datos'}
            
            cursor = connection.cursor(dictionary=True)
            
            query = "SELECT url, title FROM Page LIMIT %s"
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return results
            
        except Error as e:
            logger.error(f"Error obteniendo páginas disponibles: {e}")
            return {'error': f'Error obteniendo páginas: {str(e)}'}

    def get_available_words(self, limit=100):
        """Obtener lista de palabras disponibles para facilitar las pruebas"""
        try:
            connection = self.get_connection()
            if not connection:
                return {'error': 'No se pudo conectar a la base de datos'}
            
            cursor = connection.cursor(dictionary=True)
            
            query = "SELECT word FROM Word ORDER BY total_repetitions DESC LIMIT %s"
            cursor.execute(query, (limit,))
            results = cursor.fetchall()
            
            cursor.close()
            connection.close()
            
            return [row['word'] for row in results]
            
        except Error as e:
            logger.error(f"Error obteniendo palabras disponibles: {e}")
            return {'error': f'Error obteniendo palabras: {str(e)}'}