# IMPORTANDO MÓDULOS E PACOTES
import pandas as pd
import numpy as np
import datetime as dt
import logging
import numpy as np
import logging 


logging.basicConfig(
    level=logging.INFO,  # Exibe mensagens a partir de INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Garante logs no console
    ]
)

class Transform:
        
        def board_status_treatment(self, df, df_conf):

            try:
                # CRIANDO LISTA PARA IDENTIFICAR POSSÍVEIS REATIVAÇÕES NO BOARD STATUS TREATMENT
                status_filter_list = ['CANCELADO', 'CANCELADA', 'FINALIZADO', 'FINALIZADA', 'NAO RENOVADO'] 

                if df is None or df.empty:
                    logging.info('\n ----------------------------------------------------------------------------------')
                    logging.info('DataFrame vazio, retornando DataFrame vazio')
                    return pd.DataFrame()

                if df_conf is None or df_conf.empty:
                    logging.info('\n ----------------------------------------------------------------------------------')
                    logging.info('DataFrame de conferência vazio, retornando DataFrame original')
                    return df

                # Verificando se as colunas necessárias existem
                required_columns = ['placa', 'chassi', 'empresa', 'status']
                missing_columns = [col for col in required_columns if col not in df.columns]
                if missing_columns:
                    logging.info('\n ----------------------------------------------------------------------------------')
                    logging.info(f'Colunas necessárias ausentes no DataFrame: {missing_columns}')
                    return df

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Shape do DataFrame antes do tratamento: {df.shape}')

                # Inicializar colunas de status e migration_from
                df['status_beneficio'] = 'NOVO'
                df['migration_from'] = np.nan

                # Agrupar dados de conferência por chassi e benefício
                df_conf_grouped = df_conf.groupby(['chassi', 'beneficio']).agg({
                    'empresa': list,
                    'data_ativacao_beneficio': list,
                    'status_beneficio': list
                }).reset_index()

                # Criar um dicionário para lookup rápido
                conf_dict = df_conf_grouped.set_index(['chassi', 'beneficio']).to_dict('index')

                # Processar cada chassi/benefício único (validação)
                for chassi, beneficio in df[['chassi', 'beneficio']].drop_duplicates().values:
                    if (chassi, beneficio) in conf_dict:
                        conf_data = conf_dict[(chassi, beneficio)]
                        
                        if len(conf_data['empresa']) > 1:
                            # Ordenar datas e status
                            dates = sorted([d for d in conf_data['data_ativacao_beneficio'] if pd.notna(d)])
                            
                            if len(dates) > 1:
                                penultima_data = dates[-2]
                                idx_penultima = conf_data['data_ativacao_beneficio'].index(penultima_data)
                                status_penultimo = conf_data['status_beneficio'][idx_penultima]
                                empresa_penultima = conf_data['empresa'][idx_penultima]
                                
                                # Atualizar registros correspondentes
                                mask = (df['chassi'] == chassi) & (df['beneficio'] == beneficio)
                                
                                if status_penultimo not in status_filter_list:
                                    if empresa_penultima != df.loc[mask, 'empresa'].iloc[0]:
                                        df.loc[mask, 'status_beneficio'] = 'MIGRAÇÃO'
                                        df.loc[mask, 'migration_from'] = empresa_penultima
                                    else:
                                        df.loc[mask, 'status_beneficio'] = 'RENOVAÇÃO'
                                else:
                                    df.loc[mask, 'status_beneficio'] = 'REATIVAÇÃO'

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Processamento concluído com sucesso!')

                return df

            except Exception as e:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Falha no tratamento de status das placas ativadas: {str(e)}')
                return df    

        def transforming_files(self, df_ativacoes, df_cancelamentos, df_conferencia):

            # DEFININDO DATAS E INICIALIZANDO DATAFRAMES FINAIS
            today = dt.date.today()
            yesterday = today - dt.timedelta(days=1)
            friday = today - dt.timedelta(days=3)
            df_final_ativacoes = pd.DataFrame()

            #TRATANDO A NOMENCLATURA DOS BENEFÍCIOS, ADICIONANDO COLUNA DE MIGRAÇÃO, FILTRANDO POR 'CASCO'/'TERCEIRO'
            try:
                df_ativacoes['data_ativacao_beneficio'] = pd.to_datetime(df_ativacoes['data_ativacao_beneficio']).dt.date
                df_ativacoes['beneficio'] = df_ativacoes['beneficio'].replace('REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'CASCO (VEÍCULO)').replace('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'CASCO (R/SR)').replace('REPARAÇÃO OU REPOSIÇÃO DO COMPLEMENTO', 'CASCO (COMPLEMENTO)')
                df_ativacoes['migration_from'] = np.nan
                df_ativacoes = df_ativacoes.loc[df_ativacoes['beneficio'].str.contains('(CASCO|TERCEIRO)', regex=True)]

                df_conferencia['beneficio'] = df_conferencia['beneficio'].replace('REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'CASCO (VEÍCULO)').replace('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'CASCO (R/SR)').replace('REPARAÇÃO OU REPOSIÇÃO DO COMPLEMENTO', 'CASCO (COMPLEMENTO)')
                
            except Exception as e:
                logging.info('\n ----------------------------------------------------------------------------------')  
                logging.info(f'Falha ao filtrar dados de cancelamentos referente ao dia anterior: {e}')

            # SELECIONANDO DADOS DE CANCELAMENTO DO DIA ANTERIOR (OU DESDE SEXTA) E ORDENANDO POR DATA
            try:
                df_cancelamentos['data_cancelamento'] = pd.to_datetime(df_cancelamentos['data_cancelamento']).dt.date
                if today.weekday() == 0:
                    df_cancelamentos = df_cancelamentos[df_cancelamentos['data_cancelamento'].between(friday, today)]
                else:
                    df_cancelamentos = df_cancelamentos[df_cancelamentos['data_cancelamento'] == yesterday]
                    
                df_cancelamentos = df_cancelamentos.sort_values(by='data_cancelamento', ascending=True)
                
            except Exception as e:
                logging.info('\n ----------------------------------------------------------------------------------')  
                logging.info(f'Falha ao filtrar dados de cancelamentos referente ao dia anterior: {e}')
         
            try:
                # SELECIONANDO DADOS DE ATIVAÇÃO DO DIA ANTERIOR (OU DESDE SEXTA), TRATANDO E CONCATENANDO COM O RESTANTE DOS DIAS
                if not df_ativacoes.empty:

                    if today.weekday() != 0:
                        df_ativacoes_menos_ontem = df_ativacoes.loc[
                        ~(df_ativacoes['data_ativacao_beneficio'].isin([yesterday, today]))
                    ]
                        df_ativacoes_dia_anterior = df_ativacoes[df_ativacoes['data_ativacao_beneficio'] == yesterday]
                    else:
                        df_ativacoes_menos_ontem = df_ativacoes.loc[
                        (df_ativacoes['data_ativacao_beneficio']<friday)
                    ]
                        df_ativacoes_dia_anterior = df_ativacoes[df_ativacoes['data_ativacao_beneficio'].between(friday, yesterday)]

                    df_ativacoes_dia_anterior_tratado = self.board_status_treatment(df=df_ativacoes_dia_anterior, df_conf=df_conferencia)
                    df_ativacoes_atualizado = pd.concat([df_ativacoes_menos_ontem, df_ativacoes_dia_anterior_tratado])
                    
                    logging.info('\n ----------------------------------------------------------------------------------')
                    logging.info(f'Número de registros ativos na carteira tratado com os dados do dia anterior.')
                    
            except Exception as e:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Falha ao incluir registros ativos referente ao dia anterior na contagem . Revise o código: {e}')

            # ÚLTIMO TRATAMENTO DO DATAFRAME DE ATIVAÇÃO
            try: 
                
                df_final_ativacoes = df_ativacoes_atualizado[[
                    'placa', 'chassi', 'id_placa', 'id_veiculo', 'id_carroceria', 'matricula', 'conjunto', 'unidade', 'consultor', 'status_beneficio', 
                    'cliente', 'data', 'data_ativacao_beneficio', 'suporte', 'data_filtro', 'empresa', 'migration_from'
                ]]
                df_final_ativacoes = df_final_ativacoes.drop_duplicates(subset='chassi') 
                
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Processo final de tratamento de dataframe de ativação realizado com sucesso!')

            except Exception as e:
                logging.info('\n ----------------------------------------------------------------------------------')
                print(f'Falha ao tratar o dataframe de ativação: {e}')

            # TRATANDO DADOS NULOS NOS DATAFRAMES
            try: 
                df_final_ativacoes['placa'] = df_final_ativacoes['placa'].fillna('SEM-PLACA')
                df_final_ativacoes['chassi'] = df_final_ativacoes['chassi'].fillna('NULL')
                df_final_ativacoes['id_placa'] = df_final_ativacoes['id_placa'].fillna(0)
                df_final_ativacoes['id_veiculo'] = df_final_ativacoes['id_veiculo'].fillna(0)
                df_final_ativacoes['id_carroceria'] = df_final_ativacoes['id_carroceria'].fillna(0)
                df_final_ativacoes['matricula'] = df_final_ativacoes['matricula'].fillna(0)
                df_final_ativacoes['conjunto'] = df_final_ativacoes['conjunto'].fillna(0)
                df_final_ativacoes['unidade'] = df_final_ativacoes['unidade'].fillna('NULL')
                df_final_ativacoes['consultor'] = df_final_ativacoes['consultor'].fillna('NULL')
                df_final_ativacoes['status'] = df_final_ativacoes['status'].fillna('NULL')
                df_final_ativacoes['cliente'] = df_final_ativacoes['cliente'].fillna('NULL')
                df_final_ativacoes['data'] = df_final_ativacoes['data'].fillna(pd.Timestamp('1900-01-01'))
                df_final_ativacoes['data_ativacao'] = df_final_ativacoes['data_ativacao'].fillna(pd.Timestamp('1900-01-01'))
                df_final_ativacoes['suporte'] = df_final_ativacoes['suporte'].fillna('NULL')
                df_final_ativacoes['data_filtro'] = df_final_ativacoes['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
                df_final_ativacoes['empresa'] = df_final_ativacoes['empresa'].fillna('NULL')

                df_cancelamentos['placa'] = df_cancelamentos['placa'].fillna('SEM-PLACA')
                df_cancelamentos['chassi'] = df_cancelamentos['chassi'].fillna('NULL')
                df_cancelamentos['id_placa'] = df_cancelamentos['id_placa'].fillna(0)
                df_cancelamentos['id_veiculo'] = df_cancelamentos['id_veiculo'].fillna(0)
                df_cancelamentos['id_carroceria'] = df_cancelamentos['id_carroceria'].fillna(0)
                df_cancelamentos['matricula'] = df_cancelamentos['matricula'].fillna(0)
                df_cancelamentos['conjunto'] = df_cancelamentos['conjunto'].fillna(0)
                df_cancelamentos['unidade'] = df_cancelamentos['unidade'].fillna('NULL')
                df_cancelamentos['status'] = df_cancelamentos['status'].fillna('NULL')
                df_cancelamentos['cliente'] = df_cancelamentos['cliente'].fillna('NULL')
                df_cancelamentos['data'] = df_cancelamentos['data'].fillna(pd.Timestamp('1900-01-01'))
                df_cancelamentos['data_cancelamento'] = df_cancelamentos['data_cancelamento'].fillna(pd.Timestamp('1900-01-01'))
                df_cancelamentos['usuario_cancelamento'] = df_cancelamentos['usuario_cancelamento'].fillna('NULL')
                df_cancelamentos['data_filtro'] = df_cancelamentos['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
                df_cancelamentos['empresa'] = df_cancelamentos['empresa'].fillna('NULL')

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info('\n Processo de Transformacao de Dados concluido com sucesso!')

            except Exception as e:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Falha ao realizar tratamento de dados: {e}')

            return df_final_ativacoes, df_cancelamentos

