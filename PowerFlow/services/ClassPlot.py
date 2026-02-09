# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# import os
# import re
# from matplotlib.collections import LineCollection

# class Plot:
#     @staticmethod
#     def MapCentroid(df: pd.DataFrame, dfsub: pd.DataFrame, DictRetorno: dict):
#         # 1. Configuração de caminhos
#         caminho_script = os.path.dirname(os.path.abspath(__file__))
#         raiz_projeto = os.path.dirname(caminho_script)
#         save_dir = os.path.join(raiz_projeto, "images", "centroide")
#         os.makedirs(save_dir, exist_ok=True)

#         # 2. Processamento das coordenadas dos trechos (df completo)
#         # Extrai as tuplas de coordenadas da string "(-15.x, -59.x), (-15.y, -59.y)"
#         def extract_points(coord_str):
#             try:
#                 matches = re.findall(r"\(([^)]+)\)", str(coord_str))
#                 p1 = [float(x) for x in matches[0].split(',')]
#                 p2 = [float(x) for x in matches[1].split(',')]
#                 # Retorna [(lon1, lat1), (lon2, lat2)] para o matplotlib
#                 return [(p1[1], p1[0]), (p2[1], p2[0])]
#             except:
#                 return None

#         lines = df['coord_latlon'].apply(extract_points).dropna().tolist()
#         lc = LineCollection(lines, colors='lightgrey', linewidths=0.5, alpha=0.7)

#         # 3. Processamento da Subestação
#         # Extrai coordenadas da sub (assumindo formato similar ao pac)
#         def extract_sub_coords(coord_str):
#             match = re.search(r"\(([^)]+)\)", str(coord_str))
#             p = [float(x) for x in match.group(1).split(',')]
#             return p[1], p[0] # lon, lat

#         sub_lon, sub_lat = extract_sub_coords(dfsub['coord_latlon'].iloc[0])

#         # 4. Filtro de cargas (usando o df processado que contém a coluna 'kw')
#         # Nota: 'df' aqui deve ser o dataframe de nós com a coluna 'kw' calculada anteriormente
#         df_cargas = df[df['kw'] > 0].copy()

#         # Função auxiliar para plotar a base (Circuito + Subestação)
#         def plot_base_circuit(ax):
#             ax.add_collection(LineCollection(lines, colors='grey', linewidths=0.6, alpha=0.4))
#             ax.scatter(sub_lon, sub_lat, c='black', s=80, marker='o', label=f"Sub: {dfsub['sub'].iloc[0]}", zorder=5)

#         # --- IMAGEM 1: Mapa de Calor ---
#         plt.figure(figsize=(12, 10))
#         ax1 = plt.gca()
#         plot_base_circuit(ax1)
#         sns.kdeplot(
#             x=df_cargas['lon'], y=df_cargas['lat'], 
#             weights=df_cargas['kw'], fill=True, cmap="YlOrRd", thresh=0.05, levels=50, alpha=0.8, ax=ax1
#         )
#         plt.title("Distribuição de Carga e Topologia do Alimentador")
#         plt.legend()
#         plt.savefig(os.path.join(save_dir, "1_mapa_calor.png"), dpi=300)
#         plt.close()

#         # --- IMAGEM 2: Centroide e Cargas ---
#         plt.figure(figsize=(12, 10))
#         ax2 = plt.gca()
#         plot_base_circuit(ax2)
#         sc = ax2.scatter(df_cargas['lon'], df_cargas['lat'], c=df_cargas['kw'], cmap='viridis', s=20, edgecolors='white', linewidths=0.3, zorder=3)
#         ax2.scatter(DictRetorno['lon'], DictRetorno['lat'], c='red', marker='*', s=300, label='Centroide de Carga', edgecolors='black', zorder=10)
#         plt.colorbar(sc, label='Carga (kW)')
#         plt.title("Topologia, Pontos de Consumo e Centroide")
#         plt.legend()
#         plt.savefig(os.path.join(save_dir, "2_centroide_e_cargas.png"), dpi=300)
#         plt.close()

#         # --- IMAGEM 3: Mapa de Calor + Centroide ---
#         plt.figure(figsize=(12, 10))
#         ax3 = plt.gca()
#         plot_base_circuit(ax3)
#         sns.kdeplot(
#             x=df_cargas['lon'], y=df_cargas['lat'], 
#             weights=df_cargas['kw'], fill=True, cmap="YlOrRd", alpha=0.5, ax=ax3
#         )
#         ax3.scatter(DictRetorno['lon'], DictRetorno['lat'], c='blue', marker='x', s=200, lw=4, label='Centroide', zorder=10)
#         plt.title("Densidade de Carga vs Centro de Gravidade")
#         plt.legend()
#         plt.savefig(os.path.join(save_dir, "3_calor_e_centroide.png"), dpi=300)
#         plt.close()



# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns
# import os
# import re
# from matplotlib.collections import LineCollection

# class Plot:
#     @staticmethod
#     def MapCentroid(df_nos: pd.DataFrame, df_trechos: pd.DataFrame, df_sub: pd.DataFrame, DictRetorno: dict):
#         # 1. Configuração de caminhos baseada na localização do script
#         caminho_script = os.path.dirname(os.path.abspath(__file__))
#         raiz_projeto = os.path.dirname(caminho_script)
#         save_dir = os.path.join(raiz_projeto, "images", "centroide")
#         os.makedirs(save_dir, exist_ok=True)

#         # 2. Processamento de Coordenadas dos Trechos (Linhas)
#         def parse_coords(s):
#             # Extrai os pares de (lat, lon) da string no formato do dataframe
#             pts = re.findall(r"\(([^)]+)\)", str(s))
#             p1 = [float(x) for x in pts[0].split(',')]
#             p2 = [float(x) for x in pts[1].split(',')]
#             # Retorna no formato (longitude, latitude) para o plot
#             return [(p1[1], p1[0]), (p2[1], p2[0])]

#         linhas = df_trechos['coord_latlon'].apply(parse_coords).tolist()
        
#         # 3. Processamento de Coordenadas da Subestação
#         sub_pts = re.findall(r"\(([^)]+)\)", str(df_sub['coord_latlon'].iloc[0]))
#         sub_coords = [float(x) for x in sub_pts[0].split(',')]
#         sub_lon, sub_lat = sub_coords[1], sub_coords[0]

#         # 4. Filtragem de barras com carga
#         df_cargas = df_nos[df_nos['kw'] > 0].copy()

#         # Função interna para desenhar a base comum a todos os gráficos
#         def desenhar_base_robusta(ax):
#             # Desenha o circuito com traço mais grosso para melhor visibilidade
#             lc = LineCollection(linhas, colors='black', linewidths=1.5, alpha=0.6, zorder=1)
#             ax.add_collection(lc)
#             # Plota a Subestação como um ponto preto sólido
#             ax.scatter(sub_lon, sub_lat, c='black', s=120, marker='o', label='Subestação', zorder=5)

#         # --- IMAGEM 1: Mapa de Densidade + Cargas + Centroide ---
#         plt.figure(figsize=(12, 10))
#         ax1 = plt.gca()
#         desenhar_base_robusta(ax1)
        
#         # Plot das cargas individuais
#         ax1.scatter(df_cargas['lon'], df_cargas['lat'], c='blue', s=30, alpha=0.5, label='Cargas (kW)', zorder=2)
        
#         # Mapa de densidade (sem o preenchimento sólido sobre o centroide)
#         sns.kdeplot(x=df_cargas['lon'], y=df_cargas['lat'], weights=df_cargas['kw'], 
#                     fill=False, cmap="Reds", alpha=0.8, levels=15, ax=ax1, zorder=3)
        
#         # Centroide plotado com cor escura e sem preenchimento colorido ao redor
#         ax1.scatter(DictRetorno['lon'], DictRetorno['lat'], c='darkred', marker='X', s=250, 
#                     label='Centroide (Carga)', zorder=10, edgecolors='black')
        
#         plt.title("1. Distribuição de Cargas, Circuito e Centroide")
#         plt.legend(loc='upper right')
#         plt.xlabel("Longitude")
#         plt.ylabel("Latitude")
#         plt.savefig(os.path.join(save_dir, "1_distribuicao_centroide.png"), dpi=300)
#         plt.close()

#         # --- IMAGEM 2: Mapa de Calor Limpo ---
#         plt.figure(figsize=(12, 10))
#         ax2 = plt.gca()
#         desenhar_base_robusta(ax2)
#         sns.kdeplot(x=df_cargas['lon'], y=df_cargas['lat'], weights=df_cargas['kw'], 
#                     fill=True, cmap="YlOrBr", alpha=0.5, thresh=0.05, ax=ax2, zorder=2)
#         plt.title("2. Mapa de Calor de Carregamento")
#         plt.savefig(os.path.join(save_dir, "2_mapa_calor_puro.png"), dpi=300)
#         plt.close()

#         # --- IMAGEM 3: Foco no Centroide (Sem preenchimento colorido) ---
#         plt.figure(figsize=(12, 10))
#         ax3 = plt.gca()
#         desenhar_base_robusta(ax3)
#         # Dispersão de cargas colorida pelo valor em kW
#         sc = ax3.scatter(df_cargas['lon'], df_cargas['lat'], c=df_cargas['kw'], cmap='viridis', s=40, zorder=3)
#         # Centroide em cor escura e destaque
#         ax3.scatter(DictRetorno['lon'], DictRetorno['lat'], c='black', marker='X', s=300, 
#                     label='Centroide Calculado', zorder=10, edgecolors='white')
        
#         plt.colorbar(sc, label='Potência (kW)')
#         plt.title("3. Centroide de Carga e Pontos Consumidores")
#         plt.legend()
#         plt.savefig(os.path.join(save_dir, "3_foco_centroide.png"), dpi=300)
#         plt.close()

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
from matplotlib.collections import LineCollection
import numpy as np

class Plot:
    @staticmethod
    def MapCentroid(df_nos: pd.DataFrame, 
                   df_trechos: pd.DataFrame, 
                   df_sub: pd.DataFrame, 
                   DictRetorno: dict, 
                   feederPath: str):
        
        # Obtém apenas o nome da última pasta (ID do alimentador)
        nome_alimentador = os.path.basename(os.path.dirname(feederPath))

        # 1. Configuração de caminhos relativa à localização do script
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        
        # Cria estrutura: raiz/images/ID_ALIMENTADOR/centroide
        save_dir = os.path.join(raiz_projeto, "images", str(nome_alimentador), "centroide")
        os.makedirs(save_dir, exist_ok=True)

        # 2. Processamento de Coordenadas dos Trechos (Linhas)
        def parse_coords(s):
            pts = re.findall(r"\(([^)]+)\)", str(s))
            # Converte strings para float (lat, lon) -> (lon, lat) para o plot
            p1 = [float(x) for x in pts[0].split(',')]
            p2 = [float(x) for x in pts[1].split(',')]
            return [(p1[1], p1[0]), (p2[1], p2[0])]

        linhas = df_trechos['coord_latlon'].apply(parse_coords).tolist()
        
        # 3. Processamento de Coordenadas da Subestação
        sub_pts = re.findall(r"\(([^)]+)\)", str(df_sub['coord_latlon'].iloc[0]))
        sub_coords = [float(x) for x in sub_pts[0].split(',')]
        sub_lon, sub_lat = sub_coords[1], sub_coords[0]

        # 4. Localização da Barra Mais Próxima (Ponto de Instalação Real)
        barra_real = df_nos[df_nos['pac'] == DictRetorno['barra_proxima']]
        real_lon, real_lat = barra_real['lon'].iloc[0], barra_real['lat'].iloc[0]

        # 5. Filtragem de barras com carga para densidade
        df_cargas = df_nos[df_nos['kw'] > 0].copy()

        # Função de base robusta para os três gráficos
        def desenhar_base_tecnica(ax):
            # Circuito com traço grosso e Subestação
            lc = LineCollection(linhas, colors='black', linewidths=1.5, alpha=0.4, zorder=1)
            ax.add_collection(lc)
            ax.scatter(sub_lon, sub_lat, c='black', s=150, marker='o', label='Subestação', zorder=5)
            
            # Centroide Teórico (X escuro)
            ax.scatter(DictRetorno['lon'], DictRetorno['lat'], c='darkred', marker='X', s=200, 
                       label='Centroide Teórico', zorder=10, edgecolors='black')
            
            # Ponto de Instalação Real (Barra física)
            ax.scatter(real_lon, real_lat, c='lime', marker='o', s=100, 
                       label='Ponto de Instalação (Barra)', zorder=11, edgecolors='black')

        # --- IMAGEM 1: Distribuição de Cargas e Localização Física ---
        plt.figure(figsize=(12, 10))
        ax1 = plt.gca()
        desenhar_base_tecnica(ax1)
        ax1.scatter(df_cargas['lon'], df_cargas['lat'], c='blue', s=25, alpha=0.3, label='Pontos de Carga', zorder=2)
        sns.kdeplot(x=df_cargas['lon'], y=df_cargas['lat'], weights=df_cargas['kw'], 
                    fill=False, cmap="Reds", alpha=0.7, levels=12, ax=ax1, zorder=3)
        plt.title(f"1. Distribuição Geográfica e Centro de Carga - {nome_alimentador}")
        plt.legend(loc='upper right')
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.savefig(os.path.join(save_dir, "1_distribuicao_instalacao.png"), dpi=300)
        plt.close()

        # --- IMAGEM 2: Mapa de Calor de Carregamento ---
        plt.figure(figsize=(12, 10))
        ax2 = plt.gca()
        desenhar_base_tecnica(ax2)
        sns.kdeplot(x=df_cargas['lon'], y=df_cargas['lat'], weights=df_cargas['kw'], 
                    fill=True, cmap="YlOrBr", alpha=0.4, thresh=0.05, ax=ax2, zorder=2)
        plt.title(f"2. Intensidade de Carga e Ponto Ótimo - {nome_alimentador}")
        plt.legend(loc='upper right')
        plt.savefig(os.path.join(save_dir, "2_calor_instalacao.png"), dpi=300)
        plt.close()

        # --- IMAGEM 3: Foco em Carregamento por Barra ---
        plt.figure(figsize=(12, 10))
        ax3 = plt.gca()
        desenhar_base_tecnica(ax3)
        sc = ax3.scatter(df_cargas['lon'], df_cargas['lat'], c=df_cargas['kw'], cmap='viridis', s=45, zorder=3)
        plt.colorbar(sc, label='Potência Ativa (kW)')
        plt.title(f"3. Barra Selecionada para Equipamento: {DictRetorno['barra_proxima']}")
        plt.legend(loc='upper right')
        plt.savefig(os.path.join(save_dir, "3_foco_tecnico_barra.png"), dpi=300)
        plt.close()