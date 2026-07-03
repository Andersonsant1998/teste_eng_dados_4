import pandas as pd
import os

caminho_csv = 'datasets/clientes_sinteticos.csv'

if os.path.exists(caminho_csv):
    df_temp = pd.read_csv(caminho_csv)
    
    # Corrige a coluna 'num_casa' como fizemos antes
    if 'num_casa_cliente' in df_temp.columns:
        df_temp = df_temp.rename(columns={'num_casa_cliente': 'num_casa'})
        
    # CONVERSÃO DE DATAS: Transforma yyyy-MM-dd para dd/MM/yyyy para o seu script ler
    # Isso faz com que o formato fique compatível com o "dd/MM/yyyy" que seu script espera
    df_temp['dt_nascimento_cliente'] = pd.to_datetime(df_temp['dt_nascimento_cliente']).dt.strftime('%d/%m/%Y')
    df_temp['dt_atualizacao'] = pd.to_datetime(df_temp['dt_atualizacao']).dt.strftime('%d/%m/%Y')
    
    df_temp.to_csv(caminho_csv, index=False)
    print("Datas e colunas corrigidas para o formato do seu script!")
else:
    print("ERRO: Arquivo não encontrado.")