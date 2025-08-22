# IMPORTANDO MÓDULOS E PACOTES
import pandas as pd
import numpy as np
import datetime as dt
import logging
import traceback
from extract import Extract

class Transform:
    def __init__(self):
        self.extract_inst = Extract()
        self.today = dt.date.today()
        self.yesterday = self.today - dt.timedelta(days=1)
        self.last_friday = self.today - dt.timedelta(days=3)
        self.comeco_campanha = dt.date(2025, 5, 1)
        self.lista_dias_faltantes = [
            dt.date(2025, 7, 20), dt.date(2025, 7, 28), dt.date(2025, 7, 29),
            dt.date(2025, 7, 30), dt.date(2025, 7, 31)
        ]
        self.date_for_all = pd.date_range(start=self.comeco_campanha, end=self.today, freq='D')
        self.status_filter_list = ['CANCELADO', 'CANCELADA', 'FINALIZADO', 'FINALIZADA', 'NAO RENOVADO']

    def board_status_treatment(self, df, df_conf, status_filter_list):
        try:
            if not df.empty:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(df.shape)
                row_count = 0
                for idx, row in df.iterrows():
                    row_count += 1
                    df_verification = df_conf[
                        (df_conf['chassi'] == row['chassi']) & (df_conf['beneficio'] == row['beneficio'])
                    ].sort_values(by='data_ativacao', ascending=True)

                    if not df_verification.empty and len(df_verification['empresa'].values) > 1:
                        hist_datas_ativacao = sorted(df_verification['data_ativacao_beneficio'].dropna().drop_duplicates().unique())

                        if len(hist_datas_ativacao) > 1:
                            penultimo_registro_data = hist_datas_ativacao[-2]
                            verification_penultima_row = df_verification.loc[df_verification['data_ativacao_beneficio'] == penultimo_registro_data]
                            
                            if verification_penultima_row['status_beneficio'].values[0] not in status_filter_list:
                                if verification_penultima_row['empresa'].values[0] != row['empresa']:
                                    df.at[idx, 'status_beneficio'] = 'MIGRAÇÃO'
                                    df.at[idx, 'migration_from'] = verification_penultima_row['empresa'].values[0]
                                else:
                                    df.at[idx, 'status_beneficio'] = 'RENOVAÇÃO'
                                    df.at[idx, 'migration_from'] = 'NULL'
                            else:
                                df.at[idx, 'status_beneficio'] = 'REATIVAÇÃO'
                                df.at[idx, 'migration_from'] = 'NULL'
                        else:
                            df.at[idx, 'status_beneficio'] = 'NOVO'
                            df.at[idx, 'migration_from'] = 'NULL'
                    else:
                        df.at[idx, 'status_beneficio'] = 'NOVO'
                        df.at[idx, 'migration_from'] = 'NULL'

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Total de linhas processadas: {row_count}')
            else:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info('Nenhum registro de ativações para tratamento de dados. Dataframe vazio!')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha no tratamento de status das placas ativadas. Revise o código: {e}')
        return df

    @staticmethod
    def decode_renegociacao(value):
        if value == 14326:
            return 'NAO'
        elif value == 14324:
            return 'SIM'
        elif value == 0:
            return 'VAZIO'
        else:
            logging.info('Código não encontrado para decodificação na coluna "Renegociacao". Revise o código:')
            logging.info(value)
            return value

    @staticmethod
    def decode_empresa(value):
        if value == 4330:
            return 'Viavante'
        elif value == 4328:
            return 'Stcoop'
        elif value == 4326:
            return 'Segtruck'
        elif value == 0:
            return 'Sem Empresa'
        else:
            logging.info('Código não encontrado para decodificação na coluna "Empresa". Revise o código:')
            logging.info(value)
            return value

    @staticmethod
    def correcao_valores_conjunto(value):
        if pd.isna(value):
            return []
        for sep in ['e', '/', '-', ' ']:
            value = value.replace(sep, ',')
        value = str(value).lower().strip()
        value = value.replace(',,', ',')
        value = value.replace('.', '')
        elementos = [v for v in value.split(',') if v.strip() != '']
        try:
            formated_value = [int(v) for v in elementos]
        except ValueError:
            formated_value = elementos
        return formated_value

    def transforming_files(self):
        try:
            # INICIALIANDO DATAFRAMES VAZIOS
            df_final_ativacoes = pd.DataFrame()
            df_final_cancelamentos = pd.DataFrame()

            # DEFININDO DATA DE INICIO DA CAMPANHA E CRIANDO DATAFRAME COM TODAS AS DATAS DE CAMPANHA
            today = self.today
            yesterday = self.yesterday
            last_friday = self.last_friday
            comeco_campanha = self.comeco_campanha
            lista_dias_faltantes = self.lista_dias_faltantes
            date_for_all = self.date_for_all

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('Falha ao criar parâmetros de data para filtragem de dataframes.')

        # EXTRAINDO DATAFRAMES E SEGMENTANDO-OS POR EMPRESA / PADRONIZANDO BENEFICIOS
        try:
            extract_inst = self.extract_inst
            df_final_cancelamentos = extract_inst.extract_all_cancelamentos()
            df_ativ_all_boards = extract_inst.extract_all_ativacoes()
            df_ativ_all_boards['data_ativacao_beneficio'] = pd.to_datetime(df_ativ_all_boards['data_ativacao_beneficio']).dt.date

            df_ativ_all_boards['beneficio'] = df_ativ_all_boards['beneficio'].replace(
                'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'CASCO (VEÍCULO)'
            ).replace(
                'REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'CASCO (R/SR)'
            ).replace(
                'REPARAÇÃO OU REPOSIÇÃO DO COMPLEMENTO', 'CASCO (COMPLEMENTO)'
            )

            df_ativ_viavante = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Viavante']
            df_ativ_stcoop = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Stcoop']
            df_ativ_segtruck = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Segtruck']
            df_ativ_tag = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Tag']

            df_conf_all_boards = extract_inst.extract_conf_boards()
            df_conf_all_boards['beneficio'] = df_conf_all_boards['beneficio'].replace(
                'REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'CASCO (VEÍCULO)'
            ).replace(
                'REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'CASCO (R/SR)'
            ).replace(
                'REPARAÇÃO OU REPOSIÇÃO DO COMPLEMENTO', 'CASCO (COMPLEMENTO)'
            )

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao realizar a segmentação dos dataframes: {e}')

        # SELECIONANDO APENAS AS ATIVAÇÕES CORRESPONDENTES AOS BENEFICIOS 'CASCO' / 'TERCEIRO' POR UM REGEX PADRÃO
        try:
            ids_beneficios_segtruck = [2, 3, 4, 7, 24, 25, 26, 29]
            ids_beneficios_stcoop = [24, 25, 26, 29]
            ids_beneficios_viavante = [40, 41, 42, 45]
            ids_beneficios_tag = [2, 3, 4, 7, 24, 25, 26, 29, 34, 35, 36, 37, 38, 39]

            df_ativ_viavante = df_ativ_viavante.loc[df_ativ_viavante['id_beneficio'].isin(ids_beneficios_viavante)]
            df_ativ_stcoop = df_ativ_stcoop[df_ativ_stcoop['id_beneficio'].isin(ids_beneficios_stcoop)]
            df_ativ_segtruck = df_ativ_segtruck.loc[df_ativ_segtruck['id_beneficio'].isin(ids_beneficios_segtruck)]
            df_ativ_tag = df_ativ_tag.loc[df_ativ_tag['id_beneficio'].isin(ids_beneficios_tag)]

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao padronizar nomenclaturas referente aos beneficios pré-estabelecidos: {e}')

        # FILTRANDO OS DADOS PELA DATA DE CANCELAMENTO / DATA DE ATUALIZAÇÃO, E ORDENANDO OS DADOS NO DATAFRAME PELAS COLUNAS DE DATA
        try:
            df_final_cancelamentos_integrais = df_final_cancelamentos
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao filtrar dados de cancelamentos referente ao dia anterior: {e}')

        # CRIANDO COLUNA DE MIGRAÇÃO (MIGRATION_FROM) E DEFININDO FILTRO DE STATUS PARA TRATAMENTO DE DADOS DA CAMPANHA
        try:
            for df in [df_ativ_tag, df_ativ_viavante, df_ativ_stcoop, df_ativ_segtruck, df_ativ_all_boards]:
                if not df.empty:
                    df['migration_from'] = np.nan
            status_filter_list = self.status_filter_list
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha na ciração das colunas de migração e definição de filtro de status: {e}')

        # GERANDO DATAFRAME FINAL (TAG)
        try:
            if not df_ativ_tag.empty:
                if today.weekday() == 0:
                    df_final_tg = df_ativ_tag[df_ativ_tag['data_ativacao_beneficio'].between(last_friday, today)]
                else:
                    df_final_tg = df_ativ_tag[df_ativ_tag['data_ativacao_beneficio'] == yesterday]
            else:
                df_final_tg = pd.DataFrame()
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Nenhum registro de ativação encontrado na Tag')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao atualizar placas referente a Tag: {e}')

        # GERANDO DATAFRAME FINAL (VIAVANTE)
        try:
            if not df_ativ_viavante.empty:
                if today.weekday() == 0:
                    df_final_viav = df_ativ_viavante[df_ativ_viavante['data_ativacao_beneficio'].between(last_friday, today)]
                else:
                    df_final_viav = df_ativ_viavante[df_ativ_viavante['data_ativacao_beneficio'] == yesterday]
            else:
                df_final_viav = pd.DataFrame()
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Nenhum registro de ativação encontrado na Viavante')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao atualizar placas referente a Viavante: {e}')

        # GERANDO DATAFRAME FINAL (STCOOP)
        try:
            if not df_ativ_stcoop.empty:
                if today.weekday() == 0:
                    df_final_st = df_ativ_stcoop[df_ativ_stcoop['data_ativacao_beneficio'].between(last_friday, today)]
                else:
                    df_final_st = df_ativ_stcoop[df_ativ_stcoop['data_ativacao_beneficio'] == yesterday]
            else:
                df_final_st = pd.DataFrame()
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Nenhum registro de ativação encontrado na Stcoop')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao atualizar as ativações referente a Stcoop: {e}')

        # GERANDO DATAFRAME FINAL (Segtruck)
        try:
            if not df_ativ_segtruck.empty:
                if today.weekday() == 0:
                    df_final_seg = df_ativ_segtruck[df_ativ_segtruck['data_ativacao_beneficio'].between(last_friday, today)]
                else:
                    df_final_seg = df_ativ_segtruck[df_ativ_segtruck['data_ativacao_beneficio'] == yesterday]
            else:
                df_final_seg = pd.DataFrame()
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Nenhum registro de ativação encontrado na Segtruck')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao atualizar as ativações referente a Segtruck: {e}')

        try:
            # PEGANDO DADOS DE ATIVAÇÃO DO DIA ANTERIOR
            if not df_ativ_all_boards.empty:
                df_ativos_menos_ontem = df_ativ_all_boards[~(df_ativ_all_boards['data_ativacao_beneficio'] == yesterday)]
                df_ativos_dia_anterior = df_ativ_all_boards[df_ativ_all_boards['data_ativacao_beneficio'] == yesterday]
                df_ativacoes_dia_anterior_ranking_tratado = self.board_status_treatment(
                    df=df_ativos_dia_anterior,
                    df_conf=df_conf_all_boards,
                    status_filter_list=status_filter_list
                )
                df_ativacoes_atualizado = pd.concat([df_ativos_menos_ontem, df_ativacoes_dia_anterior_ranking_tratado])
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Número de registros ativos na carteira tratado com os dados do dia anterior.')
            else:
                df_ativacoes_atualizado = pd.DataFrame()
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao incluir registros ativos referente ao dia anterior na contagem . Revise o código: {e}')

        # CRIANDO DATAFRAMES FINAIS: ATIVAÇÕES E CANCELAMENTOS
        try:
            # APLICANDO FUNÇÃO DE TRATAMENTO DE STATUS DAS PLACAS ATIVADAS
            df_final_viavante = self.board_status_treatment(df=df_final_viav, df_conf=df_conf_all_boards, status_filter_list=status_filter_list)
            df_final_stcoop = self.board_status_treatment(df=df_final_st, df_conf=df_conf_all_boards, status_filter_list=status_filter_list)
            df_final_segtruck = self.board_status_treatment(df=df_final_seg, df_conf=df_conf_all_boards, status_filter_list=status_filter_list)
            df_final_tag = self.board_status_treatment(df=df_final_tg, df_conf=df_conf_all_boards, status_filter_list=status_filter_list)

            # CONCATENANDO DATAFRAMES DE ATIVAÇÕES DA CAMPANHA
            df_ativacoes_dia_anterior_ranking = pd.concat([df_final_viavante, df_final_stcoop, df_final_segtruck, df_final_tag])
            df_ativacoes_dia_anterior_ranking = df_ativacoes_dia_anterior_ranking.sort_values(by='data_ativacao_beneficio', ascending=True)

            # DEFININDO COLUNAS QUE SERÃO UTILIZADAS NOS DATAFRAMES FINAIS
            df_final_ativacoes = df_ativacoes_atualizado[[
                'placa', 'chassi', 'id_placa', 'id_veiculo', 'id_carroceria', 'matricula', 'conjunto', 'unidade', 'consultor', 'status_beneficio',
                'cliente', 'data_registro', 'data_ativacao_beneficio', 'suporte', 'data_filtro', 'empresa', 'migration_from'
            ]]

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Processo de Concatenação de Dataframes realizado com sucesso!')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao unir os dataframes: {e}')

        # TRATANDO NÚMERO DE REGISTROS ATIVOS / RETIRANDO DUPLICATAS
        try:
            df_final_ativacoes = df_final_ativacoes.drop_duplicates(subset='chassi')
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Número de registros ativos tratado e corrigido com sucesso.')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao tratar e corrigir número de registros ativos. Revise o código: {e}')

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
            df_final_ativacoes['status_beneficio'] = df_final_ativacoes['status_beneficio'].fillna('NULL')
            df_final_ativacoes['cliente'] = df_final_ativacoes['cliente'].fillna('NULL')
            df_final_ativacoes['data_registro'] = df_final_ativacoes['data_registro'].fillna(pd.Timestamp('1900-01-01'))
            df_final_ativacoes['data_ativacao_beneficio'] = df_final_ativacoes['data_ativacao_beneficio'].fillna(pd.Timestamp('1900-01-01'))
            df_final_ativacoes['suporte'] = df_final_ativacoes['suporte'].fillna('NULL')
            df_final_ativacoes['data_filtro'] = df_final_ativacoes['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
            df_final_ativacoes['empresa'] = df_final_ativacoes['empresa'].fillna('NULL')
            df_final_ativacoes['migration_from'] = df_final_ativacoes['migration_from'].fillna('NULL')

            df_final_cancelamentos_integrais['placa'] = df_final_cancelamentos_integrais['placa'].fillna('SEM-PLACA')
            df_final_cancelamentos_integrais['chassi'] = df_final_cancelamentos_integrais['chassi'].fillna('NULL')
            df_final_cancelamentos_integrais['id_placa'] = df_final_cancelamentos_integrais['id_placa'].fillna(0)
            df_final_cancelamentos_integrais['id_veiculo'] = df_final_cancelamentos_integrais['id_veiculo'].fillna(0)
            df_final_cancelamentos_integrais['id_carroceria'] = df_final_cancelamentos_integrais['id_carroceria'].fillna(0)
            df_final_cancelamentos_integrais['matricula'] = df_final_cancelamentos_integrais['matricula'].fillna(0)
            df_final_cancelamentos_integrais['conjunto'] = df_final_cancelamentos_integrais['conjunto'].fillna(0)
            df_final_cancelamentos_integrais['unidade'] = df_final_cancelamentos_integrais['unidade'].fillna('NULL')
            df_final_cancelamentos_integrais['status'] = df_final_cancelamentos_integrais['status'].fillna('NULL')
            df_final_cancelamentos_integrais['cliente'] = df_final_cancelamentos_integrais['cliente'].fillna('NULL')
            df_final_cancelamentos_integrais['data'] = df_final_cancelamentos_integrais['data'].fillna(pd.Timestamp('1900-01-01'))
            df_final_cancelamentos_integrais['data_cancelamento'] = df_final_cancelamentos_integrais['data_cancelamento'].fillna(pd.Timestamp('1900-01-01'))
            df_final_cancelamentos_integrais['usuario_cancelamento'] = df_final_cancelamentos_integrais['usuario_cancelamento'].fillna('NULL')
            df_final_cancelamentos_integrais['data_filtro'] = df_final_cancelamentos_integrais['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
            df_final_cancelamentos_integrais['empresa'] = df_final_cancelamentos_integrais['empresa'].fillna('NULL')
            df_final_cancelamentos_integrais['migracao'] = df_final_cancelamentos_integrais['migracao'].fillna('NULL')
            df_final_cancelamentos_integrais['renegociacao'] = df_final_cancelamentos_integrais['renegociacao'].fillna('NULL')
            df_final_cancelamentos_integrais['data_substituicao'] = df_final_cancelamentos_integrais['data_substituicao'].fillna(pd.Timestamp('1900-01-01'))

            df_ativacoes_dia_anterior_ranking['placa'] = df_ativacoes_dia_anterior_ranking['placa'].fillna('SEM-PLACA')
            df_ativacoes_dia_anterior_ranking['chassi'] = df_ativacoes_dia_anterior_ranking['chassi'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['id_placa'] = df_ativacoes_dia_anterior_ranking['id_placa'].fillna(0)
            df_ativacoes_dia_anterior_ranking['id_veiculo'] = df_ativacoes_dia_anterior_ranking['id_veiculo'].fillna(0)
            df_ativacoes_dia_anterior_ranking['id_carroceria'] = df_ativacoes_dia_anterior_ranking['id_carroceria'].fillna(0)
            df_ativacoes_dia_anterior_ranking['matricula'] = df_ativacoes_dia_anterior_ranking['matricula'].fillna(0)
            df_ativacoes_dia_anterior_ranking['conjunto'] = df_ativacoes_dia_anterior_ranking['conjunto'].fillna(0)
            df_ativacoes_dia_anterior_ranking['unidade'] = df_ativacoes_dia_anterior_ranking['unidade'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['consultor'] = df_ativacoes_dia_anterior_ranking['consultor'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['status'] = df_ativacoes_dia_anterior_ranking['status'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['cliente'] = df_ativacoes_dia_anterior_ranking['cliente'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['data'] = df_ativacoes_dia_anterior_ranking['data'].fillna(pd.Timestamp('1900-01-01'))
            df_ativacoes_dia_anterior_ranking['data_ativacao'] = df_ativacoes_dia_anterior_ranking['data_ativacao'].fillna(pd.Timestamp('1900-01-01'))
            df_ativacoes_dia_anterior_ranking['suporte'] = df_ativacoes_dia_anterior_ranking['suporte'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['data_filtro'] = df_ativacoes_dia_anterior_ranking['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
            df_ativacoes_dia_anterior_ranking['empresa'] = df_ativacoes_dia_anterior_ranking['empresa'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['migration_from'] = df_ativacoes_dia_anterior_ranking['migration_from'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['coverage'] = df_ativacoes_dia_anterior_ranking['coverage'].fillna(0)
            df_ativacoes_dia_anterior_ranking['beneficio'] = df_ativacoes_dia_anterior_ranking['beneficio'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['categoria'] = df_ativacoes_dia_anterior_ranking['categoria'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['tipo_categoria'] = df_ativacoes_dia_anterior_ranking['tipo_categoria'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['data_ativacao_beneficio'] = df_ativacoes_dia_anterior_ranking['data_ativacao_beneficio'].fillna(pd.Timestamp('1900-01-01'))
            df_ativacoes_dia_anterior_ranking['status_beneficio'] = df_ativacoes_dia_anterior_ranking['status_beneficio'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['data_atualizacao'] = df_ativacoes_dia_anterior_ranking['data_atualizacao'].fillna(pd.Timestamp('1900-01-01'))
            df_ativacoes_dia_anterior_ranking['microfranquia'] = df_ativacoes_dia_anterior_ranking['microfranquia'].fillna('NULL')
            df_ativacoes_dia_anterior_ranking['id_beneficio'] = df_ativacoes_dia_anterior_ranking['id_beneficio'].fillna(0)

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Processo de Transformacao de Dados concluido com sucesso!')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao realizar tratamento de dados: {e}')

        return df_final_ativacoes, df_final_cancelamentos_integrais, df_ativacoes_dia_anterior_ranking