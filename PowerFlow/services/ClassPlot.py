
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
from matplotlib.collections import LineCollection

class Plot:
    @staticmethod
    def MapCentroid(df_nos: pd.DataFrame, 
                   df_trechos: pd.DataFrame, 
                   df_sub: pd.DataFrame, 
                   DictRetorno: dict, 
                   feederPath: str):
        
        nome_alimentador = os.path.basename(os.path.dirname(feederPath))
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", str(nome_alimentador), "centroide")
        os.makedirs(save_dir, exist_ok=True)


        def parse_coords(s):
            pts = re.findall(r"\(([^)]+)\)", str(s))
            p1 = [float(x) for x in pts[0].split(',')]
            p2 = [float(x) for x in pts[1].split(',')]
            return [(p1[1], p1[0]), (p2[1], p2[0])]

        linhas = df_trechos['coord_latlon'].apply(parse_coords).tolist()
        sub_pts = re.findall(r"\(([^)]+)\)", str(df_sub['coord_latlon'].iloc[0]))
        sub_coords = [float(x) for x in sub_pts[0].split(',')]
        sub_lon, sub_lat = sub_coords[1], sub_coords[0]


        barra_reguladores = DictRetorno.get('barra_reguladores', [])
        if isinstance(barra_reguladores, str): barra_reguladores = [barra_reguladores]
        
        barra_capacitores = DictRetorno.get('barra_capacitores', [])
        if isinstance(barra_capacitores, str): barra_capacitores = [barra_capacitores]

        df_cargas = df_nos[df_nos['kw'] > 0].copy()

        def desenhar_base_tecnica(ax):
            lc = LineCollection(linhas, colors='black', linewidths=1.5, alpha=0.7, zorder=1)
            ax.add_collection(lc)
            ax.scatter(sub_lon, sub_lat, c='black', s=150, marker='o', label='Subestação', zorder=5)
            
            # Centroide Teórico
            ax.scatter(DictRetorno['lon'], DictRetorno['lat'], c='darkred', marker='X', s=200, 
                       label='Baricentro de Carga', zorder=10, edgecolors='black')
            
            # Plot de todos os reguladores (Pontos Verdes)
            primeiro_reg = True
            for b_id in barra_reguladores:
                barra_real = df_nos[df_nos['pac'] == b_id]
                if not barra_real.empty:
                    label = 'Reguladores de Tensão' if primeiro_reg else None
                    ax.scatter(barra_real['lon'].iloc[0], barra_real['lat'].iloc[0], 
                               c='lime', marker='o', s=100, label=label, zorder=15, edgecolors='black')
                    primeiro_reg = False
                    
            
            # Plot de todos os capacitores (Pontos Azuis)
            primeiro_cap = True
            for b_id in barra_capacitores:
                barra_real = df_nos[df_nos['pac'] == b_id]
                if not barra_real.empty:
                    label = 'Bancos de Capacitores' if primeiro_cap else None
                    ax.scatter(barra_real['lon'].iloc[0], barra_real['lat'].iloc[0], 
                               c='blue', marker='o', s=100, label=label, zorder=15, edgecolors='black')
                    primeiro_cap = False
                    

        plt.figure(figsize=(12, 10))
        ax1 = plt.gca()
        desenhar_base_tecnica(ax1)
        ax1.scatter(df_cargas['lon'], df_cargas['lat'], c='blue', s=25, alpha=0.3, label='Cargas de Média Tensão', zorder=2)
        sns.kdeplot(x=df_cargas['lon'], y=df_cargas['lat'], weights=df_cargas['kw'], 
                    fill=False, cmap="Reds", alpha=0.7, levels=12, ax=ax1, zorder=3)
        plt.title(f"Baricentro de Carga Alimentador: {nome_alimentador}")
        plt.legend(loc='upper right')
        plt.savefig(os.path.join(save_dir, "Baricentro.png"), dpi=300)
        plt.close()


        plt.figure(figsize=(12, 10))
        ax2 = plt.gca()
        desenhar_base_tecnica(ax2)
        sns.kdeplot(x=df_cargas['lon'], y=df_cargas['lat'], weights=df_cargas['kw'], 
                    fill=True, cmap="YlOrBr", alpha=0.4, thresh=0.05, ax=ax2, zorder=2)
        plt.title(f"Mapa de Calor das Cargas Alimentador: {nome_alimentador}")
        plt.legend(loc='upper right')
        plt.savefig(os.path.join(save_dir, "Mapa_Calor.png"), dpi=300)
        plt.close()


        plt.figure(figsize=(12, 10))
        ax3 = plt.gca()
        desenhar_base_tecnica(ax3)
        sc = ax3.scatter(df_cargas['lon'], df_cargas['lat'], c=df_cargas['kw'], cmap='viridis', s=45, zorder=3)
        plt.colorbar(sc, label='Potência Ativa (kW)')
        plt.title(f"3. Localização dos {len(barra_reguladores)} Reguladores de Tensão e\n {len(barra_capacitores)} Bancos de Capacitores: {nome_alimentador}")
        plt.legend(loc='upper right')
        plt.savefig(os.path.join(save_dir, "ReguladoresECapacitores.png"), dpi=300)
        plt.close()








    @staticmethod
    def PlotCaminhoVermelho(df_nos: pd.DataFrame, 
                            df_trechos: pd.DataFrame, 
                            DictRetorno: dict, 
                            feederPath: str):

        
        nome_alimentador = os.path.basename(os.path.dirname(feederPath))
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", str(nome_alimentador), "centroide")
        os.makedirs(save_dir, exist_ok=True)

        plt.figure(figsize=(12, 10))
        tronco_set = set(DictRetorno.get('pacs_tronco', []))
        
        lista_p1_reg, lista_p2_reg, lista_p1_BkShunt, lista_p2_BkShunt = DictRetorno.get('ramo', ([], [], [], []))
        
        ramos_reguladores = set()
        for p1, p2 in zip(lista_p1_reg, lista_p2_reg):
            if p1 and p2:
                ramos_reguladores.add(tuple(sorted([str(p1).lower(), str(p2).lower()])))

        ramos_bkshunt = set()
        for p1, p2 in zip(lista_p1_BkShunt, lista_p2_BkShunt):
            if p1 and p2:
                ramos_bkshunt.add(tuple(sorted([str(p1).lower(), str(p2).lower()])))

        # 2. Desenho da Rede com Destaques
        for _, row in df_trechos.iterrows():
            p1, p2 = str(row['pac_1']).lower(), str(row['pac_2']).lower()
            par_atual = tuple(sorted([p1, p2]))
            
            n1 = df_nos[df_nos['pac'] == p1]
            n2 = df_nos[df_nos['pac'] == p2]
            
            if not n1.empty and not n2.empty:
                cor, largura, alpha, z = 'black', 0.6, 0.2, 1
                
                # Verifica se faz parte do tronco (caminho vermelho)
                if p1 in tronco_set and p2 in tronco_set:
                    try:
                        idx1 = DictRetorno['pacs_tronco'].index(p1)
                        idx2 = DictRetorno['pacs_tronco'].index(p2)
                        if abs(idx1 - idx2) == 1: 
                            cor, largura, alpha, z = 'red', 2.5, 0.8, 10
                    except ValueError: pass
                

                if par_atual in ramos_reguladores:
                    cor, largura, alpha, z = 'yellow', 6.0, 1.0, 20
                    
                if par_atual in ramos_bkshunt:
                    cor, largura, alpha, z = 'orange', 6.0, 1.0, 20

                plt.plot([n1['lon'].values[0], n2['lon'].values[0]], 
                         [n1['lat'].values[0], n2['lat'].values[0]], 
                         color=cor, linewidth=largura, alpha=alpha, zorder=z)


        plt.scatter(DictRetorno['lon'], DictRetorno['lat'], color='blue', s=100, 
                    marker='X', label='Baricentro de Carga', zorder=25, edgecolors='white')
        

        barras_alvo = DictRetorno.get('barra_proxima', [])
        if isinstance(barras_alvo, str): barras_alvo = [barras_alvo]
        
        primeiro_reg = True
        for b_id in barras_alvo:
            barra_f = df_nos[df_nos['pac'] == b_id]
            if not barra_f.empty:
                label = 'Barras de Instalação Reguladores' if primeiro_reg else None
                plt.scatter(barra_f['lon'].iloc[0], barra_f['lat'].iloc[0], 
                            color='lime', s=120, label=label, zorder=30, edgecolors='black')
                primeiro_reg = False
                
        
        primeiro_BkShunt = True
        for b_id in barras_alvo:
            barra_f = df_nos[df_nos['pac'] == b_id]
            if not barra_f.empty:
                label = 'Barras de Instalação BkShunt' if primeiro_BkShunt else None
                plt.scatter(barra_f['lon'].iloc[0], barra_f['lat'].iloc[0], 
                            color='lime', s=120, label=label, zorder=30, edgecolors='black')
                primeiro_BkShunt = False

        plt.plot([], [], color='red', linewidth=2.5, label='Tronco Principal')
        plt.plot([], [], color='yellow', linewidth=5, label='Ramos c/ Reguladores')
        plt.plot([], [], color='orange', linewidth=5, label='Ramos c/ BkShunt')
        plt.title(f"Reguladores e Bancos de Capacitor a instalar: {nome_alimentador}")
        plt.xlabel("Longitude")
        plt.ylabel("Latitude")
        plt.legend(loc='upper right')
        plt.grid(True, linestyle='--', alpha=0.3)

        save_path = os.path.join(save_dir, "MenorCaminhoAteBaricentro.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
