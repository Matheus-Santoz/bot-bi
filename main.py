import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests_oauthlib import OAuth2Session
import os
import time
from datetime import datetime

CLIENT_ID = os.environ.get("BLING_CLIENT_ID")
CLIENT_SECRET = os.environ.get("BLING_CLIENT_SECRET")
NOME_PLANILHA = 'Teste power bi'
CREDENCIAIS_GOOGLE_JSON = os.environ.get("GOOGLE_CREDENTIALS")

MARGEM_SEGURANCA_SEGUNDOS = 600

def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(CREDENCIAIS_GOOGLE_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def gerenciar_token(acao, token_novo=None):
    client = conectar_google()
    
    try:
        sheet = client.open(NOME_PLANILHA).worksheet('Config')
    except Exception as e:
        print(f"ERRO CRITICO: Nao consegui abrir a aba Config. Erro: {e}")
        raise

    if acao == 'ler':
        try:
            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            sheet.update_acell('E1', f"Bot Ativo em: {agora}")
            print(f"Permissao de escrita OK. Data registrada em E1: {agora}")
        except Exception as e:
            print(f"ERRO DE PERMISSAO: O bot le mas nao consegue escrever. Erro: {e}")

        token_texto = sheet.acell('B1').value
        if not token_texto:
            raise Exception("A celula B1 esta vazia.")
        return json.loads(token_texto.replace("'", '"'))
    
    if acao == 'salvar':
        try:
            print("Tentando salvar novo token na planilha...")
            sheet.update_acell('B1', json.dumps(token_novo))
            
            agora = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
            sheet.update_acell('C1', 'Ultima Renovacao:')
            sheet.update_acell('D1', agora)
            
            print(f"SUCESSO! Token e Data ({agora}) salvos na planilha.")
        except Exception as e:
            print(f"ERRO CRITICO AO SALVAR TOKEN: {e}")

def buscar_compras():
    print("Conectando ao Bling...")
    token = gerenciar_token('ler')
    
    token_url = 'https://www.bling.com.br/Api/v3/oauth/token'
    refresh_url = 'https://www.bling.com.br/Api/v3/oauth/token'
    extra = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}

    precisa_renovar = False
    
    if 'expires_at' in token:
        tempo_agora = time.time()
        tempo_expiracao = token['expires_at']
        tempo_restante = tempo_expiracao - tempo_agora
        
        print(f"Token expira em: {int(tempo_restante/60)} minutos.")

        if tempo_restante < MARGEM_SEGURANCA_SEGUNDOS:
            print("Token proximo do fim.")
            precisa_renovar = True
    else:
        print("Token SEM data de expiracao.")
        precisa_renovar = True

    if precisa_renovar:
        print("Iniciando renovacao forcada...")
        try:
            bling_temp = OAuth2Session(CLIENT_ID, token=token)
            novo_token = bling_temp.refresh_token(token_url, **extra)
            
            gerenciar_token('salvar', novo_token)
            token = novo_token
            print("Renovacao concluida.")
        except Exception as e:
            print(f"Falha na renovacao preventiva: {e}")
            print("Tentando seguir com o token atual mesmo assim...")

    bling = OAuth2Session(CLIENT_ID, token=token, 
                          auto_refresh_url=refresh_url, 
                          auto_refresh_kwargs=extra, 
                          token_updater=lambda t: gerenciar_token('salvar', t))

    url = 'https://www.bling.com.br/Api/v3/pedidos/vendas'
    
    resp = bling.get(url)
    
    if resp.status_code == 200:
        return resp.json().get('data', [])
    else:
        print(f"Erro ao buscar vendas no Bling: {resp.status_code}")
        print(f"Resposta: {resp.text}")
        return []

def enviar_dados(dados_bling):
    if not dados_bling: 
        print("Nenhum dado de vendas retornado.")
        return

    client = conectar_google()

    try:
        try:
            sheet = client.open(NOME_PLANILHA).worksheet('Página1')
        except:
            sheet = client.open(NOME_PLANILHA).sheet1
    except Exception as e:
        print(f"Erro ao abrir aba de dados: {e}")
        return

    coluna_ids = sheet.col_values(1) 
    ids_na_planilha = set(coluna_ids)
    
    print(f"Verificando duplicidade contra {len(ids_na_planilha)} vendas existentes...")

    novas_linhas = []
    
    for venda in dados_bling:
        id_venda = str(venda.get('id'))

        if id_venda not in ids_na_planilha:
            novas_linhas.append([
                venda.get('id'),
                venda.get('numero'),
                venda.get('data'),
                venda.get('contato', {}).get('nome'),
                venda.get('total'),
                venda.get('situacao', {}).get('valor') 
            ])

    if novas_linhas:
        if len(ids_na_planilha) <= 1 and "ID" not in ids_na_planilha:
             sheet.append_row(["ID", "Número", "Data", "Cliente", "Valor Total", "Situação"])

        sheet.append_rows(novas_linhas)
        print(f"SUCESSO! {len(novas_linhas)} novas vendas adicionadas.")
    else:
        print("Tudo atualizado. Nenhuma venda nova.")

if __name__ == '__main__':
    dados = buscar_compras()
    enviar_dados(dados)
