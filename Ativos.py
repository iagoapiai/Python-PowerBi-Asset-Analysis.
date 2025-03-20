import math
import decimal
import psycopg2
import datetime
import numpy as np
import pandas as pd
import boto3.dynamodb.types
from datetime import datetime, timedelta

# Connections ---------------------

boto3.dynamodb.types.DYNAMODB_CONTEXT = decimal.Context(prec=100)

session = boto3.session.Session(#CONFIDENTIAL)

connection = psycopg2.connect(#CONFIDENTIAL)

# Func. Endpoints ---------------------  

def replace_non_alphabetic(value):
    if pd.isna(value):
        return "Indefinido" 
    value_str = str(value)
    return "Indefinido" if not any(char.isalpha() for char in value_str) else value_str
            
def converter_data(value):
    try:
        data_formatada = pd.to_datetime(value)
        nova_data = data_formatada - timedelta(hours=3)
        formato_data_hora = nova_data.strftime('%d/%m/%Y %H:%M')
        return formato_data_hora
    except ValueError:
        return None
            
def epoch_converter(value):
    if value is not None:
        value = int(value)
        if value is not None and not math.isnan(value):
            try:
                data_hora = datetime.fromtimestamp(value / 1000)
                formato_data_hora = data_hora.strftime('%d/%m/%Y %H:%M')
                return formato_data_hora
            except Exception as e:
                print(f"Erro na conversão: {e}")
                return None
        else:
            pass
        
def rssi_converter(value):
    if len(str(value)) < 4:
        value = value * 10
    return value

def hyperlink(value):
    value = f"https://retina.ibbx.tech/positions/{value}?tab=global"
    return value 

def map_sensor_type(label):
    if pd.isna(label):
        return "Outro"
    label = label.lower()
    if any(keyword in label for keyword in ["vazão", "velocidade", "volume", "consumo"]):
        return "Vazão / Hidrômetro"
    elif "nível" in label:
        return "Nível"
    elif "status" in label:
        return "ON/OFF"
    elif "pressão" in label:
        return "Pressão"
    elif "temperatura" in label:
        return "Temperatura Mod Bus"
    elif any(keyword in label for keyword in ["tensão", "totalizador", "corrente"]):
        return "Multimedidor"
    else:
        return "Outro"
    
def extrair_carteira(obs):
    if obs is None:
        return 'Vazio'
    obs_lower = obs.lower()
    if 'carteira 1' in obs_lower:
        return 'Carteira 1'
    elif 'carteira 2' in obs_lower:
        return 'Carteira 2'
    elif 'carteira 3' in obs_lower:
        return 'Carteira 3'
    elif 'carteira' in obs_lower:
        return 'Carteira'
    else:
        return 'Vazio'

def remover_carteira(obs):
    if not obs:
        return 'Vazio'
    palavras_removidas = ['carteira 1', 'carteira 2', 'carteira 3', 'carteira']
    obs_lower = obs.lower()
    for palavra in palavras_removidas:
        obs_lower = obs_lower.replace(palavra, '')
    obs_lower = obs_lower.replace(',', '').strip()
    if not obs_lower: 
        return 'Vazio'
    return obs_lower.title()

# Func. Bolts -----------------------

def epoch_converter_bolt(value):
    if not math.isnan(value):
        value = int(value)
        data_hora = datetime.fromtimestamp(value / 1000.0)
        formato_data_hora = data_hora.strftime('%d/%m/%Y %H:%M')
        return formato_data_hora
    return None

def clean_string(s):
    if isinstance(s, str):
        return ''.join(c for c in s if c.isprintable())
    return s

# Bolts Dynamo -----------------------

dyna = session.resource('dynamodb')
table = dyna.Table('retina-configurator-gateways')
table_scan = table.scan(Select="ALL_ATTRIBUTES")
df_bruto = pd.json_normalize(table_scan['Items'], sep='_')
df_bruto['facilityId'] = pd.to_numeric(df_bruto['facilityId'], errors='coerce')

df_bolts = pd.DataFrame()
df_bolts['Id'] = df_bruto['gatewayId']
df_bolts['Ultima Tx'] = df_bruto['healthCheckDate']
df_bolts['FacilityId'] = df_bruto['facilityId']

df_bolts['Ultima Tx'] = pd.to_datetime(df_bolts['Ultima Tx'].apply(epoch_converter_bolt),format='%d/%m/%Y %H:%M',errors='coerce')

data_atual = datetime.now()
diferenca_tempo = timedelta(minutes=30)

df_bolts['Connected'] = df_bolts['Ultima Tx'].apply(lambda row: "Não" if pd.isnull(row) or data_atual - row > diferenca_tempo else "Sim")

df_bolts = df_bolts.applymap(clean_string)

df_bolts = df_bolts[df_bolts['Connected'] == "Não"]

df_bolts = df_bolts.groupby('FacilityId').agg({'Ultima Tx': lambda x: ', '.join(x.dropna().dt.strftime('%Y-%m-%d %H:%M'))}).reset_index()

df_bolts.rename(columns={'Ultima Tx': 'Ultimas Tx'}, inplace=True)
df_bolts = df_bolts[df_bolts['Ultimas Tx'] != '']

df_bolts = df_bolts.explode('Ultimas Tx')

# Query Endpoints -----------------------

sql_query = """
SELECT
    c.name AS "Empresa",
    c.id AS "CompanyId",
    c.complement AS "Observação da Empresa",
    f.name AS "Unidade",
    f.id AS "FacilityId",
    a.name AS "Equipamento",
    a.id AS "AssetId",
	sa.name as "Tipo de Equipamento",
    p.name AS "Ponto",
    p."countAcquisitions" AS "N° Coletas",
    s.name AS "SensorType",
    b."activatorId" AS "UUID",
    b."updatedAt" AS "Ativado",
    p."lastAcquisitionDate" AS "Ultimo Sinal",
    p."batteryVoltage" AS "Tensão Atual",
    p.id AS "Position",
    r.name AS "Representante",
    s.id AS "SensorTypeId",
    sb.id AS "SensorBoardId",
    fs.name AS "Setor",
    p.details->'measures'->0->>'label' AS "Label",  
    se.report->>'batteryLifeForecast' AS "Data Bateria",
    se.report->>'initialBatteryVoltage' AS "Tensão Inicial",
    se.report->>'temperature' AS "Temperatura",
    se.report->>'lastCollectRSSI' AS "RSSI", 
    se.report->>'gatewayId' AS "GatewayId",
    se.report->>'firmware' AS "Firmware",
    se.report->>'batteryConsumption' AS "Consumo",
    se.report->>'isRetrofit' AS "Retrofit",
    se.report->>'hardwareVersion' AS "Hardware",
    se.report->>'batch' AS "Lote",
	se.report->>'acquisitionInterval' AS "Tempo de Coleta",
	(se.report->>'spectralWindow')::INTEGER / 2 AS "Janela Spectral",
	se.report->>'isGatewayOnFacility' AS "Gateway na Unidade",
	se.report->>'isGatewayConnected' AS "Gateway Online",
    u.name AS "User Ativação",
    ru.name AS "User Representante",
	u.profile AS "User Profile"
FROM "tbPosition" p
LEFT JOIN "tbBoard" b ON b.id = p."boardId"
LEFT JOIN "tbSysBoardType" sb ON sb.id = b."sysBoardTypeId"
LEFT JOIN "tbAsset" a ON a.id = b."assetId"
LEFT JOIN "tbSysAssetType" sa ON sa.id = a."sysAssetTypeId"
LEFT JOIN "tbFacility" f ON f.id = a."facilityId"
LEFT JOIN "tbCompany" c ON c.id = f."tenantId"
LEFT JOIN "tbRepresentative" r ON r.id = c."representativeId"
LEFT JOIN "tbSysSensorType" s ON s.id = p."sysSensorTypeId"
LEFT JOIN "tbFacilitySector" fs ON fs.id = a."facilitySectorId"
LEFT JOIN "tbSummaryEndpoint" se ON se.report->>'id' = b."activatorId" AND se.report->>'type' = 'DAILY'
LEFT JOIN "tbUser" u ON u.id = b."updatedBy"
LEFT JOIN "tbRepresentative" ru ON ru.id = u."representativeId"
WHERE p."deletedAt" IS NULL
  AND p."visible" = TRUE
  AND c."skipMetrics" = FALSE
  AND b."activatorId" IS NOT NULL
"""

df_endpoint = pd.read_sql_query(sql_query, connection)

# Manipulando os dados da query  -----------------------

df_endpoint['SensorType'] = df_endpoint.apply(lambda row: map_sensor_type(row['Label']) if row['SensorType'] == 'Mod Bus' else row['SensorType'], axis=1)
df_endpoint['SensorType'] = df_endpoint.apply(lambda row: "Spectra IBBX EX" if row['SensorBoardId'] == 5 and row['SensorTypeId'] == 1 else row['SensorType'], axis=1)

df_endpoint['Data Bateria'] = df_endpoint['Data Bateria'].fillna(0)
df_endpoint['Data Bateria'] = df_endpoint['Data Bateria'].apply(epoch_converter)
df_endpoint['Data Bateria'] = pd.to_datetime(df_endpoint['Data Bateria'], format='%d/%m/%Y %H:%M', errors='coerce')

df_endpoint['Ativado'] = df_endpoint['Ativado'].apply(converter_data)
df_endpoint['Ultimo Sinal'] = df_endpoint['Ultimo Sinal'].apply(converter_data)

cols_numeric = ['Tensão Inicial', 'Tensão Atual', 'Temperatura', 'Consumo', 'SensorBoardId', 'SensorTypeId']
df_endpoint[cols_numeric] = df_endpoint[cols_numeric].apply(pd.to_numeric, errors='coerce')

df_endpoint['Ativado'] = pd.to_datetime(df_endpoint['Ativado'], format='%d/%m/%Y %H:%M')
df_endpoint['Ultimo Sinal'] = pd.to_datetime(df_endpoint['Ultimo Sinal'], format='%d/%m/%Y %H:%M')

df_endpoint['diferenca_tempo'] = datetime.now() - df_endpoint['Ultimo Sinal']
df_endpoint['Conectado'] = np.where(df_endpoint['diferenca_tempo'] < timedelta(hours=3), "Sim", "Não")
df_endpoint = df_endpoint.drop(columns=['diferenca_tempo'])

df_endpoint["Tempo de Vida (dias)"] = (df_endpoint["Ultimo Sinal"] - df_endpoint["Ativado"]).dt.days
df_endpoint.loc[df_endpoint["Tempo de Vida (dias)"] < 0, "Tempo de Vida (dias)"] = 0

df_endpoint['RSSI'] = pd.to_numeric(df_endpoint['RSSI'], errors='coerce')

df_endpoint['QR Code'] = df_endpoint['UUID'].str[-6:]

df_endpoint['Carteira'] = df_endpoint['Observação da Empresa'].apply(extrair_carteira)
df_endpoint['Observação Empresa'] = df_endpoint['Observação da Empresa'].apply(remover_carteira)

columns = ["Empresa", "Unidade", "Equipamento", "Ponto", "Position", "Conectado",
            "RSSI", "Data Bateria", "Tensão Inicial", "Tensão Atual", "Consumo",
            "Temperatura", "SensorType", "QR Code", "Ativado", "Ultimo Sinal", 
            "GatewayId", "Firmware", "Representante","CompanyId", "FacilityId", 
            "AssetId", "UUID", "N° Coletas", "Setor","Observação Empresa", "Carteira",
            "Hardware", "Retrofit", "Lote", "Tempo de Coleta", "Janela Spectral", 
            "Gateway na Unidade", 'Tempo de Vida (dias)', 'Gateway Online', 'User Ativação', 
            'User Representante', 'User Profile', 'Tipo de Equipamento']

df_endpoint = df_endpoint[columns]
 
df_endpoint['RSSI'] = df_endpoint['RSSI'].apply(rssi_converter)
df_endpoint['RSSI'] = df_endpoint['RSSI'].fillna(0)
df_endpoint['Firmware'] = df_endpoint['Firmware'].apply(replace_non_alphabetic)

# Inserindo Versão dos Sensores  -----------------------

fwc_new = ['FwC-1.7', 'FwC-1.6', 'FwC-1.7-rc0']

df_endpoint['Versão'] = df_endpoint.apply(lambda row: "Indefinido" if row['Firmware'] == "Indefinido" or row['SensorType'] != "Vibração e temperatura" else None, axis=1)
df_endpoint['Versão'] = df_endpoint.apply(lambda row: "1.1" if row['Firmware'] not in fwc_new and row['SensorType'] == "Vibração e temperatura" else row['Versão'], axis=1)
df_endpoint['Versão'] = df_endpoint.apply(lambda row: "1.2" if row['Firmware'] in fwc_new and row['SensorType'] == "Vibração e temperatura" else row['Versão'], axis=1)
df_endpoint['Versão'] = df_endpoint.apply(lambda row: "1.3" if row['Hardware'] == "1.3" and row['SensorType'] == "Vibração e temperatura" else row['Versão'], axis=1)

# Inserindo Dados de Instalação  -----------------------

df_endpoint['Implantação'] = df_endpoint.apply(lambda row: "Cliente" if row['User Profile'] in ['CLIENTE_MASTER_VIEW', 'CLIENTE_MASTER', 'CLIENTE_COMUM', 'MESA_ANALISTA'] else "Representante", axis=1)
df_endpoint['Implantação'] = df_endpoint.apply(lambda row: "Ibbx" if row['User Profile'] in ['MESA_MASTER', 'EXECUTIVO_MASTER'] else row['Implantação'], axis=1)

# Inserindo Ocorrências Luminárias -----------------------

sql_query_oc = """
SELECT DISTINCT
  a.id as "AssetId",
  TO_CHAR(ao."createdAt" AT TIME ZONE 'UTC' - INTERVAL '3 hours', 'YYYY-MM-DD HH24:MI:SS') AS "DataCriação",
  od.diagnostic as "Diagnóstico"
FROM "tbAssetOccurrence" ao                     
JOIN "tbAsset" a ON a.id = ao."assetId" AND a."deletedAt" IS NULL
LEFT JOIN "tbAssetOccurrenceDiagnostic" od ON od."assetOccurrenceId" = ao.id AND od."deletedAt" IS NULL
LEFT JOIN "tbAssetType" sa ON sa.id = a."assetTypeId"
WHERE ao."createdAt" <= CURRENT_DATE - INTERVAL '5 days'
  AND ao."closedAt" IS NULL
  AND ao."deletedAt" IS NULL
  AND sa.name = 'Luminária Pública'
  AND od.diagnostic = 'Provável defeito na Luminária'
"""

df_oc_luminaria = pd.read_sql_query(sql_query_oc, connection)

df_endpoint = df_endpoint.merge(df_oc_luminaria, on='AssetId', how='left')

# Aplicação das regras dos probemas -----------------------

TempNeg = ["ice"]

df_endpoint['Problema'] = "Normal"
df_endpoint['Problema Secundário'] = ""
df_endpoint['Problema Terciário'] = ""
df_endpoint['Problema Quaternário'] = ""

def atualizar_problema(row, novo_problema):
    if row['Problema'] not in ["Normal", "Desconectado"]:
        problemas_anteriores = [row[col] for col in ['Problema Secundário', 'Problema Terciário', 'Problema Quaternário'] if row[col]]
        problemas_anteriores.insert(0, row['Problema'])
        problemas_anteriores = list(dict.fromkeys(problemas_anteriores))  
        
        row['Problema'] = novo_problema
        for i, col in enumerate(['Problema Secundário', 'Problema Terciário', 'Problema Quaternário']):
            row[col] = problemas_anteriores[i] if i < len(problemas_anteriores) else ""
    else:
        row['Problema'] = novo_problema
    return row

# DADOS REMOVIDOS POR SEREM CONFIDENCIAIS!!!

df_endpoint[['Tensão Inicial', 'Tensão Atual', 'Temperatura', 'Consumo']] = df_endpoint[['Tensão Inicial', 'Tensão Atual', 'Temperatura', 'Consumo']].round({'Tensão Inicial': 2, 'Tensão Atual': 2, 'Temperatura': 0, 'Consumo': 3})

df_endpoint['Link'] = df_endpoint['Position'].apply(hyperlink)
 
# Manipulando Endpoints + Func(Bolts) -----------------------

sensores_conectados = df_endpoint[df_endpoint['Conectado'] == 'Sim']

sensores_desconectados = df_endpoint[df_endpoint['Conectado'] == 'Não']

df_bolts['Ultimas Tx'] = df_bolts['Ultimas Tx'].str.split(', ')

df_bolts = df_bolts.explode('Ultimas Tx')

df_final = sensores_desconectados.merge(df_bolts, on='FacilityId', how='left')

df_final = df_final.groupby('FacilityId')['Ultimas Tx'].apply(lambda x: ', '.join(x.dropna().unique())).reset_index()

df_final = sensores_desconectados.merge(df_final, on='FacilityId', how='left')

def check_gateway_offline(row):
    if pd.isna(row['Ultimo Sinal']) or pd.isna(row['Ultimas Tx']):
        return row['Problema']
    try:
        ultimas_tx_list = [pd.to_datetime(tx.strip(), errors='coerce') for tx in row['Ultimas Tx'].split(',')]
        for tx_time in ultimas_tx_list:
            if tx_time and abs((row['Ultimo Sinal'] - tx_time).total_seconds()) <= 12 * 3600:
                return 'Gateway Offline'
    except Exception as e:
        print(f"Erro ao processar linha: {row}\nErro: {e}")
    return row['Problema']  

df_final['Problema'] = df_final.apply(check_gateway_offline, axis=1)

df_final = pd.concat([df_final, sensores_conectados], axis=0)

# Alumas novas regras de problemas -----------------------

# DADOS REMOVIDOS POR SEREM CONFIDENCIAIS!!!

# Regras de ação -----------------------

# DADOS REMOVIDOS POR SEREM CONFIDENCIAIS!!!

# Removendo colunas desnecessárias -----------------------

df_final = df_final.drop_duplicates(subset='Position', keep='last')
df_final = df_final.drop(columns=['Ultimas Tx', 'DataCriação', 'Diagnóstico'])
