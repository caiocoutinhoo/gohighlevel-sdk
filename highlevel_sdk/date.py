import time
from datetime import datetime, timedelta

class DateUtil:
    """
    Classe de utilitário para trabalhar com datas.
    """
    @staticmethod
    def convert_date_to_timestamp(date_str):
        """
        Converte uma data no formato 'YYYY-MM-DD' para timestamp (milissegundos).
        
        Args:
            date_str (str): A data a ser convertida, no formato 'YYYY-MM-DD'.
        
        Returns:
            int: O timestamp correspondente.
        """
        date = datetime.strptime(date_str, "%Y-%m-%d")
        return int(time.mktime(date.timetuple()) * 1000)  # Convertendo para milissegundos

    @staticmethod
    def get_next_seven_days_timestamp(start_date):
        """
        Obtém os timestamps para o intervalo de 7 dias após a data fornecida.
        
        Args:
            start_date (str): A data de início no formato 'YYYY-MM-DD'.
        
        Returns:
            tuple: Uma tupla contendo o timestamp de início e de fim (7 dias após o início).
        """
        start_timestamp = DateUtil.convert_date_to_timestamp(start_date)
        end_timestamp = start_timestamp + 7 * 24 * 60 * 60 * 1000  # 7 dias em milissegundos
        return start_timestamp, end_timestamp
