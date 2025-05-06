import awswrangler as awr
import logging
import os
import logging

logging.basicConfig(
    level=logging.INFO,  # Exibe mensagens a partir de INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Garante logs no console
    ]
)

logging.info('\n ----------------------------------------------------------------------------------')
logging.info('\n Executando Rotina - RELATORIO ATIVACOES PENDENTES')

class Extract:

    # FUNÇÃO CONSTRUTORA
    def __init__(self):
        #self.path = os.path.dirname(__file__) #atributo de instância que obtém o caminho do diretório dirname
        self.path = r"C:\Users\raphael.almeida\Documents\Processos\placas_acompanhamento"

    # FUNÇÃO PARA EXTRAÇÃO DE DADOS DE: ATIVAÇÕES
    def extract_all_ativacoes(self): #df_ativacoes

        try:

            dir_query = os.path.join(self.path,'sql', 'all_boards_ATIVOS.sql')

            with open(dir_query, 'r') as file:
                query = file.read()

            df_ativacoes = awr.athena.read_sql_query(query, database='silver')
        
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Relatorio ativacoes (Vivante)  - Dados Extraidos com sucesso!')

            return df_ativacoes

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha ao Extrair relatorio ativacoes (Viavante): {e}')

    # FUNÇÃO PARA EXTRAÇÃO DE DADOS DE: CANCELAMENTO INTEGRAL
    def extract_all_cancelamentos(self): #df_cancelamentos

        try:

            dir_query = os.path.join(self.path,'sql', 'all_boards_CANCELADOS.sql')

            with open(dir_query, 'r') as file:
                query = file.read()
                df_cancelamentos = awr.athena.read_sql_query(query, database='silver')

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Relatorio cancelamentos  - Dados Extraidos com sucesso!')

            return df_cancelamentos

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha ao Extrair relatorio cancelamentos: {e}')

    # FUNÇÃO PARA EXTRAÇÃO DE DADOS DE: CONFERÊNCIA
    def extract_conf_boards(self): #df_conferencia

        try:

            dir_query = os.path.join(self.path,'sql', 'listagem_mestra.sql')

            with open(dir_query, 'r') as file:
                query = file.read()

            df_conferencia = awr.athena.read_sql_query(query, database='silver')
        
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Relatorio conferência  - Dados Extraidos com sucesso!')

            return df_conferencia

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha ao Extrair relatorio conferência: {e}')

    # FUNÇÃO DE EXTRAÇÃO DE DADOS DE: CANCELAMENTOS PARCIAIS
    def extract_canc_parciais(self): #df_canc_parciais

            try:

                dir_query = os.path.join(self.path,'sql', 'all_boards_CANCELAMENTOS_PARCIAIS.sql')

                with open(dir_query, 'r') as file:
                    query = file.read()

                df_canc_parciais = awr.athena.read_sql_query(query, database='silver')

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info('\n Relatorio cancelamentos parciais  - Dados Extraidos com sucesso!')

                return df_canc_parciais

            except Exception as e:

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'\n Falha ao Extrair relatorio cancelamentos parciais (Stcoop): {e}')