import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests_oauthlib import OAuth2Session
import os
import time

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
    except:
        print(f"ERRO: Não encontrei a aba 'Config' na planilha {NOME_PLANILHA}")
        raise

    if acao == 'ler':
        token_texto = sheet.acell('B1').value
        return json.loads(token_texto.replace("'", '"'))
    
    if acao == 'salvar':
        try:
            sheet.update_acell('B1', json.dumps(token_novo))
            print("--- TOKEN SALVO COM SUCESSO NA PLANILHA ---")
        except Exception as e:
            print(f"ERRO CRÍTICO AO SALVAR TOKEN: {e}")

def buscar_compras():
    print("Conectando ao Bling...")
    token = gerenciar_token('ler')
    
    token_url = 'https://www.bling.com.br/Api/v3/oauth/token'
    refresh_url = 'https://www.bling.com.br/Api/v3/oauth/token'
    extra = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}

    if 'expires_at' in token:
        tempo_agora = time.time()
        tempo_expiracao = token['expires_at']
        tempo_restante = tempo_expiracao - tempo_agora
        
        print(f"Tempo restante do token: {int(tempo_restante/60)} minutos.")

        if tempo_restante < MARGEM_SEGURANCA_SEGUNDOS:
            print("Token próximo do fim. Force renovação")

            try:
                bling_temp = OAuth2Session(CLIENT_ID, token=token)
                novo_token = bling_temp.refresh_token(token_url, **extra)
                
                gerenciar_token('salvar', novo_token)
                token = novo_token
                print("Token renovado")
            except Exception as e:
                print(f"Erro ao renovar antecipadamente: {e}")
    else:
        print("Token sem data de expiração. Forçando renovação...")
        try:
            bling_temp = OAuth2Session(CLIENT_ID, token=token)
            novo_token = bling_temp.refresh_token(token_url, **extra)
            gerenciar_token('salvar', novo_token)
            token = novo_token
        except Exception as e:
             print(f"Erro na renovação forçada: {e}")

    bling = OAuth2Session(CLIENT_ID, token=token, 
                          auto_refresh_url=refresh_url, 
                          auto_refresh_kwargs=extra, 
                          token_updater=lambda t: gerenciar_token('salvar', t))

    url = 'https://www.bling.com.br/Api/v3/pedidos/vendas'
    
    resp = bling.get(url)
    
    if resp.status_code == 200:
        return resp.json().get('data', [])
    else:
        print(f"Erro Bling: {resp.text}")
        return []

def enviar_dados(dados_bling):
    if not dados_bling: 
        print("Nenhum dado encontrado no Bling.")
        return

    client = conectar_google()

    try:
        try:
            sheet = client.open(NOME_PLANILHA).worksheet('Página1')
        except:
            sheet = client.open(NOME_PLANILHA).sheet1
    except Exception as e:
        print(f"Erro ao abrir planilha de dados: {e}")
        return

    coluna_ids = sheet.col_values(1) 
    ids_na_planilha = set(coluna_ids)
    
    print(f"Verificando duplicidade contra {len(ids_na_planilha)} vendas...")

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
