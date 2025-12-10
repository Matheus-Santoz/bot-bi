import json
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from requests_oauthlib import OAuth2Session
import os

CLIENT_ID = os.environ.get("BLING_CLIENT_ID")
CLIENT_SECRET = os.environ.get("BLING_CLIENT_SECRET")
NOME_PLANILHA = 'Teste power bi'

CREDENCIAIS_GOOGLE_JSON = os.environ.get("GOOGLE_CREDENTIALS")

def conectar_google():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds_dict = json.loads(CREDENCIAIS_GOOGLE_JSON)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    return gspread.authorize(creds)

def gerenciar_token(acao, token_novo=None):
    """Lê ou Grava o token na aba 'Config' da planilha"""
    client = conectar_google()
    sheet = client.open(NOME_PLANILHA).worksheet('Config')
    
    if acao == 'ler':
        token_texto = sheet.acell('B1').value
        return json.loads(token_texto.replace("'", '"'))
    
    if acao == 'salvar':
        sheet.update_acell('B1', json.dumps(token_novo))

def buscar_compras():
    print("Conectando ao Bling...")
    token = gerenciar_token('ler')
    
    refresh_url = 'https://www.bling.com.br/Api/v3/oauth/token'
    extra = {'client_id': CLIENT_ID, 'client_secret': CLIENT_SECRET}

    bling = OAuth2Session(CLIENT_ID, token=token, 
                          auto_refresh_url=refresh_url, 
                          auto_refresh_kwargs=extra, 
                          token_updater=lambda t: gerenciar_token('salvar', t))

    url = 'https://www.bling.com.br/Api/v3/pedidos/vendas'
    resp = bling.get(url)
    
    if resp.status_code == 200:
        return resp.json().get('data', [])
    else:
        print(f"Erro: {resp.text}")
        return []

def enviar_dados(dados):
    if not dados: 
        print("Nenhum dado encontrado para enviar.")
        return
    lista_organizada = []
    for venda in dados:
        lista_organizada.append({
            "ID": venda.get('id'),
            "Número": venda.get('numero'),
            "Data": venda.get('data'),
            "Cliente": venda.get('contato', {}).get('nome'),
            "Valor Total": venda.get('total'),
            "Situação": venda.get('situacao', {}).get('valor') 
        })

    df = pd.DataFrame(lista_organizada)
    
    client = conectar_google()
    try:
        sheet = client.open(NOME_PLANILHA).worksheet('Página1')
    except:
        sheet = client.open(NOME_PLANILHA).sheet1
        
    sheet.clear()
    sheet.update([df.columns.values.tolist()] + df.values.tolist())
    print(f"Sucesso! {len(df)} vendas enviadas para a planilha.")

if __name__ == '__main__':
    dados = buscar_compras()

    enviar_dados(dados)


