"""
Autor: Iuri Lorenzo Quirino Moraes Silva
20/01/2026
"""

from services.ClassPlot import Plot
import os 
import polars as pl

# 1. Definição dos caminhos base
caminho_script = os.path.dirname(os.path.abspath(__file__))
Ses_dir = os.path.join(caminho_script, "output")

def processar_e_gerar_graficos(mes):
    if not os.path.exists(Ses_dir):
        print(f"Diretório não encontrado: {Ses_dir}")
        return

    for subestacao in os.listdir(Ses_dir):
        caminho_sub = os.path.join(Ses_dir, subestacao)
        
        if os.path.isdir(caminho_sub):
            for alimentador in os.listdir(caminho_sub):
                caminho_ali = os.path.join(caminho_sub, alimentador)
                
                if os.path.isdir(caminho_ali):
                    # Acessa a pasta do mês (ex: "9")
                    caminho_mes = os.path.join(caminho_ali, str(mes))
                    arquivo_feather = os.path.join(caminho_mes, "dados_HCCasoBase.feather")
                    
                    if os.path.exists(arquivo_feather):
                        try:
                            df_hc = pl.read_ipc(arquivo_feather)
                            
                            print(f"Processando: {subestacao} > {alimentador} (Mês {mes})")
                            
                            # Chamada enviando os nomes das pastas para a organização das imagens
                            Plot.PlotHCTensao(
                                df=df_hc, 
                                subestacao=subestacao, 
                                alimentador=alimentador
                            )
                            
                            Plot.PlotHCPotencia(
                                df=df_hc, 
                                subestacao=subestacao, 
                                alimentador=alimentador
                            )
                            
                            Plot.PlotHCTensaoMedia(
                                df=df_hc, 
                                subestacao=subestacao, 
                                alimentador=alimentador
                            )
                            
                            Plot.PlotHCTapsRegulador(
                                df=df_hc, 
                                subestacao=subestacao, 
                                alimentador=alimentador
                            )
                            
                            Plot.PlotHCEstagiosCapacitores(
                                df=df_hc, 
                                subestacao=subestacao, 
                                alimentador=alimentador
                            )
                            
                        except Exception as e:
                            print(f"Erro em {alimentador}: {e}")
                    else:
                        print(f"Aviso: Arquivo não encontrado em {arquivo_feather}")

# Para executar o processo
if __name__ == "__main__":
    processar_e_gerar_graficos(mes=9)