
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import re
from matplotlib.collections import LineCollection
import polars as pl
import numpy as np
import ast

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


    @staticmethod
    def PlotHCTensao(df: pl.DataFrame, subestacao: str, alimentador: str):
        """
        Plota as curvas de tensão máxima (Vmax) suavizadas e amarradas à curva base (Inc 0).
        Elimina gaps e linhas retas noturnas.
        """
        # 1. Configuração de Caminhos
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", subestacao, alimentador, "HC")
        os.makedirs(save_dir, exist_ok=True)

        # 2. Tratamento Estatístico de Outliers (Z-Score > 1 conforme seu código)
        df_clean = df.with_columns([
            pl.col("Vmax").mean().over("Incremento").alias("v_mean_stat"),
            pl.col("Vmax").std().over("Incremento").alias("v_std_stat")
        ]).with_columns(
            pl.when(
                (pl.col("Vmax") > pl.col("v_mean_stat") + 10 * pl.col("v_std_stat")) |
                (pl.col("Vmax") < pl.col("v_mean_stat") - 10 * pl.col("v_std_stat"))
            )
            .then(pl.col("v_mean_stat"))
            .otherwise(pl.col("Vmax"))
            .alias("Vmax_filt")
        )

        # 3. Dicionário da Curva Base (Incremento 0) para amarração de borda
        df_base = df_clean.filter(pl.col("Incremento") == 0.0).sort("SimulPoint")
        dict_base = dict(zip(df_base["SimulPoint"], df_base["Vmax_filt"]))

        # 4. Identificação de Incrementos válidos (< 1.5 pu)
        limite_filtro = 1.5
        valid_increments = (
            df_clean.group_by("Incremento")
            .agg(pl.col("Vmax_filt").max().alias("max_v"))
            .filter(pl.col("max_v") < limite_filtro)
            .sort("Incremento")["Incremento"].to_list()
        )

        if not valid_increments:
            return

        # 5. Preparação do Gráfico
        plt.figure(figsize=(18, 6))
        colors = plt.cm.turbo(np.linspace(0, 0.9, len(valid_increments)))
        
        # 6. Plotagem com Amarração e Suavização
        for idx, inc in enumerate(valid_increments):
            df_inc = df_clean.filter(pl.col("Incremento") == inc).sort("SimulPoint")
            
            if inc > 0:
                # Filtra apenas onde houve injeção real de MMGD
                df_geracao = df_inc.filter(pl.col("PacotesInstalados") > 0)
                if df_geracao.is_empty(): continue

                p_ini = df_geracao["SimulPoint"].min()
                p_fim = df_geracao["SimulPoint"].max()
                
                # PONTOS DE AMARRAÇÃO: Valor da curva base no início e fim da janela solar
                transicao_ini = pl.DataFrame({"SimulPoint": [p_ini], "Vmax_filt": [dict_base.get(p_ini)]})
                transicao_fim = pl.DataFrame({"SimulPoint": [p_fim], "Vmax_filt": [dict_base.get(p_fim)]})
                
                # Remove pontos de borda originais para garantir conexão perfeita
                df_corpo = df_geracao.filter((pl.col("SimulPoint") > p_ini) & (pl.col("SimulPoint") < p_fim))
                
                plot_data = pl.concat([
                    transicao_ini, 
                    df_corpo.select(["SimulPoint", "Vmax_filt"]), 
                    transicao_fim
                ]).sort("SimulPoint")
            else:
                plot_data = df_inc

            x = plot_data["SimulPoint"].to_numpy()
            y = plot_data["Vmax_filt"].to_numpy()
            
            # Plotagem por blocos diários para evitar retas na madrugada
            for start, end in [(0, 96), (96, 192), (192, 288)]:
                mask = (x >= start) & (x < end)
                if np.any(mask):
                    plt.plot(x[mask], y[mask], color=colors[idx], linewidth=1.5, 
                            label=f"Inc: {inc:.2f}" if (start == 0) else "", zorder=idx)
            
            # Rótulo ao final da curva
            if len(x) > 0:
                plt.text(x[-1], y[-1], f" {inc:.2f}", fontsize=7, color=colors[idx], 
                        va='center', ha='left', fontweight='bold')

        # 7. Configuração Visual e Eixo X
        ticks_3h = np.arange(0, 289, 12)
        labels_3h = [f"{int((t % 96) / 4):02d}:00" for t in ticks_3h]
        plt.xticks(ticks_3h, labels_3h, fontsize=8, rotation=45)
        plt.xlim(0, 315)
        
        for vline in [96, 192]:
            plt.axvline(x=vline, color='black', linestyle='-', alpha=0.1)

        plt.axhline(y=1.05, color='gray', linestyle='-', linewidth=2, alpha=0.5, label="Limite 1.05 pu")

        plt.title(f"HC Vmax Suavizado (Sem Gaps) - SE: {subestacao} | Ali: {alimentador}", loc='left', fontsize=13)
        plt.xlabel("Tempo (Dia Útil | Sábado | Domingo)")
        plt.ylabel("Tensão Máxima (pu)")
        plt.grid(True, linestyle=':', alpha=0.4)
        plt.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize='small')

        # 8. Salvamento
        save_path = os.path.join(save_dir, "Evolucao_Tensao_HC_Caso_Base.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        
        
    @staticmethod
    def PlotHCTensaoMedia(df: pl.DataFrame, subestacao: str, alimentador: str):
        """
        Plota as curvas de tensão média (Vmean) suavizadas.
        Elimina gaps e linhas retas noturnas amarrando as curvas à base (Inc 0).
        """
        # 1. Configuração de Caminhos
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", subestacao, alimentador, "HC")
        os.makedirs(save_dir, exist_ok=True)

        # 2. Tratamento Estatístico de Outliers (Z-Score > 1 conforme solicitado)
        df_clean = df.with_columns([
            pl.col("Vmean").mean().over("Incremento").alias("v_mean_avg"),
            pl.col("Vmean").std().over("Incremento").alias("v_mean_std")
        ]).with_columns(
            pl.when(
                (pl.col("Vmean") > pl.col("v_mean_avg") + 1 * pl.col("v_mean_std")) |
                (pl.col("Vmean") < pl.col("v_mean_avg") - 1 * pl.col("v_mean_std"))
            )
            .then(pl.col("v_mean_avg"))
            .otherwise(pl.col("Vmean"))
            .alias("Vmean_filt")
        )

        # 3. Criar dicionário da Curva Base (Incremento 0) para amarração total
        df_base = df_clean.filter(pl.col("Incremento") == 0.0).sort("SimulPoint")
        dict_base = dict(zip(df_base["SimulPoint"], df_base["Vmean_filt"]))

        # 4. Identificação de Incrementos e Preparação do Gráfico
        valid_increments = df_clean["Incremento"].unique().sort().to_list()
        plt.figure(figsize=(18, 6))
        colors = plt.cm.turbo(np.linspace(0, 0.9, len(valid_increments)))
        
        # 5. Plotagem com Amarração e Suavização
        for idx, inc in enumerate(valid_increments):
            df_inc = df_clean.filter(pl.col("Incremento") == inc).sort("SimulPoint")
            
            if inc > 0:
                # Filtra apenas onde houve injeção real de MMGD
                df_geracao = df_inc.filter(pl.col("PacotesInstalados") > 0)
                if df_geracao.is_empty(): continue

                p_ini = df_geracao["SimulPoint"].min()
                p_fim = df_geracao["SimulPoint"].max()
                
                # PONTOS DE AMARRAÇÃO: Valor da curva base no início e fim da janela solar
                transicao_ini = pl.DataFrame({"SimulPoint": [p_ini], "Vmean_filt": [dict_base.get(p_ini)]})
                transicao_fim = pl.DataFrame({"SimulPoint": [p_fim], "Vmean_filt": [dict_base.get(p_fim)]})
                
                # Remove pontos de borda originais para garantir conexão perfeita
                df_corpo = df_geracao.filter((pl.col("SimulPoint") > p_ini) & (pl.col("SimulPoint") < p_fim))
                
                plot_data = pl.concat([
                    transicao_ini, 
                    df_corpo.select(["SimulPoint", "Vmean_filt"]), 
                    transicao_fim
                ]).sort("SimulPoint")
            else:
                plot_data = df_inc

            x = plot_data["SimulPoint"].to_numpy()
            y = plot_data["Vmean_filt"].to_numpy()
            
            # Plotagem descontínua por dias (Útil, Sábado, Domingo)
            for start, end in [(0, 96), (96, 192), (192, 288)]:
                mask = (x >= start) & (x < end)
                if np.any(mask):
                    plt.plot(x[mask], y[mask], color=colors[idx], linewidth=1.5, 
                            label=f"Inc: {inc:.2f}" if (start == 0) else "", zorder=idx)
            
            # Rótulo ao final da curva
            if len(x) > 0:
                plt.text(x[-1], y[-1], f" {inc:.2f}", fontsize=7, color=colors[idx], 
                        va='center', ha='left', fontweight='bold')

        # 6. Configuração Visual e Eixos
        ticks_3h = np.arange(0, 289, 12)
        labels_3h = [f"{int((t % 96) / 4):02d}:00" for t in ticks_3h]
        plt.xticks(ticks_3h, labels_3h, fontsize=8, rotation=45)
        plt.xlim(0, 310)
        
        for vline in [96, 192]:
            plt.axvline(x=vline, color='black', linestyle='-', alpha=0.1)

        plt.title(f"Evolução da Tensão Média Suavizada - SE: {subestacao} | Ali: {alimentador}", loc='left', fontsize=13)
        plt.ylabel("Vmean (pu)")
        plt.grid(True, linestyle=':', alpha=0.3)
        plt.legend(loc='upper left', bbox_to_anchor=(1.01, 1), fontsize='small', title="Incrementos")

        # 7. Salvamento
        save_path = os.path.join(save_dir, "Evolucao_Tensao_Media_HC_Caso_Base.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        

    @staticmethod
    def PlotHCPotencia(df: pl.DataFrame, subestacao: str, alimentador: str):
        # 1. Configuração de Caminhos e Filtro Estatístico
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", subestacao, alimentador, "HC")
        os.makedirs(save_dir, exist_ok=True)

        # Filtro Z-Score para remover picos de divergência (image_4ba82b.png)
        df_clean = df.with_columns([
            pl.col("PkW").mean().over("Incremento").alias("p_mean"),
            pl.col("PkW").std().over("Incremento").alias("p_std")
        ]).with_columns(
            pl.when(pl.col("PkW").abs() > (pl.col("p_mean").abs() + 3 * pl.col("p_std")))
            .then(pl.col("p_mean")).otherwise(pl.col("PkW")).alias("P_filt")
        )

        # 2. Criar dicionário da Curva Base (Incremento 0) para amarração total
        df_base = df_clean.filter(pl.col("Incremento") == 0.0).sort("SimulPoint")
        dict_base = dict(zip(df_base["SimulPoint"], df_base["P_filt"]))

        valid_increments = df_clean["Incremento"].unique().sort().to_list()
        plt.figure(figsize=(18, 6))
        colors = plt.cm.turbo(np.linspace(0, 0.9, len(valid_increments)))
        
        # 3. Plotagem com Amarração de Borda (Eliminação de Gaps)
        for idx, inc in enumerate(valid_increments):
            df_inc = df_clean.filter(pl.col("Incremento") == inc).sort("SimulPoint")
            
            if inc > 0:
                # Filtra onde há injeção de MMGD
                df_geracao = df_inc.filter(pl.col("PacotesInstalados") > 0)
                if df_geracao.is_empty(): continue

                # Pega o primeiro e último ponto exato da geração solar
                p_ini = df_geracao["SimulPoint"].min()
                p_fim = df_geracao["SimulPoint"].max()
                
                # CRÍTICO: Cria pontos de conexão usando o valor da linha base NO MESMO PONTO
                # Isso garante que a linha colorida encoste na preta (image_4f5d85.jpg)
                transicao_ini = pl.DataFrame({"SimulPoint": [p_ini], "P_filt": [dict_base.get(p_ini)]})
                transicao_fim = pl.DataFrame({"SimulPoint": [p_fim], "P_filt": [dict_base.get(p_fim)]})
                
                # Removemos os pontos originais das bordas para substituir pelos de transição
                df_corpo = df_geracao.filter((pl.col("SimulPoint") > p_ini) & (pl.col("SimulPoint") < p_fim))
                
                plot_data = pl.concat([
                    transicao_ini, 
                    df_corpo.select(["SimulPoint", "P_filt"]), 
                    transicao_fim
                ]).sort("SimulPoint")
            else:
                plot_data = df_inc

            x = plot_data["SimulPoint"].to_numpy()
            y = plot_data["P_filt"].to_numpy()
            
            # Plotagem por blocos diários para evitar retas na madrugada (image_4f52e0.jpg)
            for start, end in [(0, 96), (96, 192), (192, 288)]:
                mask = (x >= start) & (x < end)
                if np.any(mask):
                    plt.plot(x[mask], y[mask], color=colors[idx], linewidth=1.5, 
                            label=f"Inc: {inc:.2f}" if (start == 0) else "", zorder=idx)

        # 4. Configurações de Eixo e Salvamento
        ticks_3h = np.arange(0, 289, 12)
        labels_3h = [f"{int((t % 96) / 4):02d}:00" for t in ticks_3h]
        plt.xticks(ticks_3h, labels_3h, fontsize=8, rotation=45)
        
        for vline in [96, 192]:
            plt.axvline(x=vline, color='black', linestyle='-', alpha=0.1)

        plt.title(f"Potência Ativa Suavizada (Sem Gaps) - SE: {subestacao}", loc='left')
        plt.ylabel("Potência Ativa (kW)")
        plt.grid(True, linestyle=':', alpha=0.3)
        plt.legend(loc='upper left', bbox_to_anchor=(1.01, 1), title="Incrementos", fontsize='small')

        plt.savefig(os.path.join(save_dir, "Evolucao_Potencia_HC_Caso_Base.png"), dpi=300, bbox_inches='tight')
        plt.close()
        
        
        


    @staticmethod
    def PlotHCTapsRegulador(df: pl.DataFrame, subestacao: str, alimentador: str):
        """
        Plota todos os reguladores encontrados na coluna 'TapsAVR' em subplots separados.
        Ajusta dinamicamente os eixos para ocupar todo o espaço útil.
        """
        # 1. Configuração de Caminhos
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", subestacao, alimentador, "HC")
        os.makedirs(save_dir, exist_ok=True)

        # 2. Identificar todos os nomes de reguladores presentes
        # Pegamos uma amostra válida para descobrir as chaves do dicionário
        sample_tap = df.filter(pl.col("TapsAVR").is_not_null())["TapsAVR"][0]
        if isinstance(sample_tap, str):
            nomes_reguladores = list(ast.literal_eval(sample_tap).keys())
        else:
            nomes_reguladores = list(sample_tap.keys())

        num_regs = len(nomes_reguladores)
        
        # 3. Função robusta para extrair o tap de um regulador específico por nome
        def extrair_tap_por_nome(tap_val, nome):
            try:
                dados = ast.literal_eval(tap_val) if isinstance(tap_val, str) else tap_val
                valor = dados.get(nome, 0)
                return int(valor[0]) if isinstance(valor, list) else int(valor)
            except:
                return 0

        # Criar colunas individuais para cada regulador
        df_clean = df
        for nome in nomes_reguladores:
            df_clean = df_clean.with_columns(
                pl.col("TapsAVR").map_elements(lambda x: extrair_tap_por_nome(x, nome), 
                                            return_dtype=pl.Int64).alias(f"Tap_{nome}")
            )

        # 4. Preparação da Grade de Subplots (n linhas, 1 coluna)
        fig, axes = plt.subplots(num_regs, 1, figsize=(18, 4 * num_regs), sharex=True)
        if num_regs == 1: axes = [axes] # Garante que axes seja sempre uma lista
        
        valid_increments = df_clean["Incremento"].unique().sort().to_list()
        colors = plt.cm.turbo(np.linspace(0, 0.9, len(valid_increments)))

        # 5. Plotagem para cada regulador
        for i, nome in enumerate(nomes_reguladores):
            ax = axes[i]
            col_name = f"Tap_{nome}"
            
            # Dicionário da Curva Base para suavização deste regulador
            df_base = df_clean.filter(pl.col("Incremento") == 0.0).sort("SimulPoint")
            dict_base = dict(zip(df_base["SimulPoint"], df_base[col_name]))

            y_min_total, y_max_total = 0, 0

            for idx, inc in enumerate(valid_increments):
                df_inc = df_clean.filter(pl.col("Incremento") == inc).sort("SimulPoint")
                
                if inc > 0:
                    df_geracao = df_inc.filter(pl.col("PacotesInstalados") > 0)
                    if df_geracao.is_empty(): continue

                    p_ini, p_fim = df_geracao["SimulPoint"].min(), df_geracao["SimulPoint"].max()
                    
                    transicao_ini = pl.DataFrame({"SimulPoint": [p_ini], col_name: [dict_base.get(p_ini, 0)]})
                    transicao_fim = pl.DataFrame({"SimulPoint": [p_fim], col_name: [dict_base.get(p_fim, 0)]})
                    
                    df_corpo = df_geracao.filter((pl.col("SimulPoint") > p_ini) & (pl.col("SimulPoint") < p_fim))
                    plot_data = pl.concat([transicao_ini, df_corpo.select(["SimulPoint", col_name]), transicao_fim]).sort("SimulPoint")
                else:
                    plot_data = df_inc

                x = plot_data["SimulPoint"].to_numpy()
                y = plot_data[col_name].to_numpy()
                
                # Atualiza limites para o ajuste de escala do eixo
                y_min_total = min(y_min_total, y.min())
                y_max_total = max(y_max_total, y.max())

                for start, end in [(0, 96), (96, 192), (192, 288)]:
                    mask = (x >= start) & (x < end)
                    if np.any(mask):
                        ax.plot(x[mask], y[mask], color=colors[idx], linewidth=1.5, 
                                label=f"Inc: {inc:.2f}" if (start == 0 and i == 0) else "", zorder=idx)

            # 6. Otimização de Espaço (Limites Dinâmicos)
            # Adiciona apenas 0.5 de margem para o tap encostar no topo/fundo
            ax.set_ylim(y_min_total - 0.5, y_max_total + 0.5)
            ax.set_ylabel(f"Tap - {nome}\n(Posição)", fontsize=10)
            ax.grid(True, linestyle=':', alpha=0.4)
            ax.set_title(f"Regulador: {nome}", loc='right', fontsize=10, color='gray')

        # 7. Configuração Global (Eixo X comum)
        ticks_3h = np.arange(0, 289, 12)
        labels_3h = [f"{int((t % 96) / 4):02d}:00" for t in ticks_3h]
        plt.xticks(ticks_3h, labels_3h, fontsize=8, rotation=45)
        plt.xlabel("Tempo (Dia Útil | Sábado | Domingo)")
        
        # Legenda única no topo
        fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.02), ncol=8, fontsize='small', title="Incrementos de HC")
        
        plt.tight_layout(rect=[0, 0, 1, 0.97]) # Ajusta layout para caber a legenda
        save_path = os.path.join(save_dir, "Evolucao_Taps_Todos_Reguladores.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()
        
        
        

    @staticmethod
    def PlotHCEstagiosCapacitores(df: pl.DataFrame, subestacao: str, alimentador: str):
        """
        Plota todos os bancos de capacitores encontrados na coluna 'EstagiosBkShunt' em subplots.
        Otimiza o espaço vertical para destacar as mudanças de estágios.
        """
        # 1. Configuração de Caminhos
        caminho_script = os.path.dirname(os.path.abspath(__file__))
        raiz_projeto = os.path.dirname(caminho_script)
        save_dir = os.path.join(raiz_projeto, "images", subestacao, alimentador, "HC")
        os.makedirs(save_dir, exist_ok=True)

        # 2. Identificar nomes dos bancos de capacitores
        sample_cap = df.filter(pl.col("EstagiosBkShunt").is_not_null())["EstagiosBkShunt"][0]
        try:
            nomes_capacitores = list(ast.literal_eval(sample_cap).keys()) if isinstance(sample_cap, str) else list(sample_cap.keys())
        except:
            print(f"Aviso: Não foi possível processar capacitores para {alimentador}")
            return

        num_caps = len(nomes_capacitores)
        if num_caps == 0: return

        # 3. Função para extrair estágio tratando erros de tipo (List vs Int)
        def extrair_estagio(cap_val, nome):
            try:
                dados = ast.literal_eval(cap_val) if isinstance(cap_val, str) else cap_val
                valor = dados.get(nome, 0)
                # Trata o caso de retornar lista [1, 1, 0...] pegando a soma ou o primeiro
                if isinstance(valor, list):
                    return int(sum(valor)) # Retorna total de estágios ativos
                return int(valor)
            except:
                return 0

        # Criar colunas individuais
        df_clean = df
        for nome in nomes_capacitores:
            df_clean = df_clean.with_columns(
                pl.col("EstagiosBkShunt").map_elements(lambda x, n=nome: extrair_estagio(x, n), 
                                                    return_dtype=pl.Int64).alias(f"Cap_{nome}")
            )

        # 4. Configuração da Grade
        fig, axes = plt.subplots(num_caps, 1, figsize=(18, 4 * num_caps), sharex=True)
        if num_caps == 1: axes = [axes]
        
        valid_increments = df_clean["Incremento"].unique().sort().to_list()
        colors = plt.cm.turbo(np.linspace(0, 0.9, len(valid_increments)))

        # 5. Loop de Plotagem por Banco
        for i, nome in enumerate(nomes_capacitores):
            ax = axes[i]
            col_name = f"Cap_{nome}"
            
            df_base = df_clean.filter(pl.col("Incremento") == 0.0).sort("SimulPoint")
            dict_base = dict(zip(df_base["SimulPoint"], df_base[col_name]))

            y_min_total, y_max_total = 0, 0

            for idx, inc in enumerate(valid_increments):
                df_inc = df_clean.filter(pl.col("Incremento") == inc).sort("SimulPoint")
                
                if inc > 0:
                    df_geracao = df_inc.filter(pl.col("PacotesInstalados") > 0)
                    if df_geracao.is_empty(): continue

                    p_ini, p_fim = df_geracao["SimulPoint"].min(), df_geracao["SimulPoint"].max()
                    
                    transicao_ini = pl.DataFrame({"SimulPoint": [p_ini], col_name: [dict_base.get(p_ini, 0)]})
                    transicao_fim = pl.DataFrame({"SimulPoint": [p_fim], col_name: [dict_base.get(p_fim, 0)]})
                    
                    df_corpo = df_geracao.filter((pl.col("SimulPoint") > p_ini) & (pl.col("SimulPoint") < p_fim))
                    plot_data = pl.concat([transicao_ini, df_corpo.select(["SimulPoint", col_name]), transicao_fim]).sort("SimulPoint")
                else:
                    plot_data = df_inc

                x = plot_data["SimulPoint"].to_numpy()
                y = plot_data[col_name].to_numpy()
                
                y_min_total = min(y_min_total, y.min())
                y_max_total = max(y_max_total, y.max())

                for start, end in [(0, 96), (96, 192), (192, 288)]:
                    mask = (x >= start) & (x < end)
                    if np.any(mask):
                        # Drawstyle 'steps-post' é ideal para capacitores (mudanças discretas)
                        ax.plot(x[mask], y[mask], color=colors[idx], linewidth=1.5, 
                                drawstyle='steps-post',
                                label=f"Inc: {inc:.2f}" if (start == 0 and i == 0) else "", zorder=idx)

            # 6. Otimização de Espaço
            ax.set_ylim(y_min_total - 0.2, y_max_total + 0.2)
            ax.set_ylabel(f"Estágios - {nome}", fontsize=10)
            ax.grid(True, linestyle=':', alpha=0.4)
            ax.set_title(f"Banco de Capacitor: {nome}", loc='right', fontsize=10, color='gray')

        # 7. Finalização
        ticks_3h = np.arange(0, 289, 12)
        labels_3h = [f"{int((t % 96) / 4):02d}:00" for t in ticks_3h]
        plt.xticks(ticks_3h, labels_3h, fontsize=8, rotation=45)
        plt.xlabel("Tempo (Dia Útil | Sábado | Domingo)")
        
        fig.legend(loc='upper center', bbox_to_anchor=(0.5, 1.02), ncol=8, fontsize='small', title="Incrementos de HC")
        
        plt.tight_layout(rect=[0, 0, 1, 0.97])
        save_path = os.path.join(save_dir, "Evolucao_Capacitores_Todos.png")
        plt.savefig(save_path, dpi=300, bbox_inches='tight')
        plt.close()