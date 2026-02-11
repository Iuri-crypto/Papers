import os
from typing import Any
import polars as pl
from pathlib import Path
import numpy as np
import gc
import networkx as nx
from scipy.spatial import cKDTree
import pandas as pd
from services.Optimizer import Optimizer
from services.ClassPlot import Plot
import ast
from geopy.distance import geodesic

class MyClassDss:
    
    
    @staticmethod       
    def RunFeeder(args: tuple) -> None:
        """ Roda o fluxo de Potência """
        
        # Parâmetros de entrada
        (CaminhoDss, 
        OutputSimul,
        MesIndex,
        ModeloCarga,
        UsarCargasBt, 
        UsarCargasMt,
        UsarMmgdBt,
        UsarMmgdMt,
        UsarPchsCghs, 
        IndexFeeder,
        ProgressDict,
        FdIrrigante, 
        Otimizar,
        ErroFluxoTolerancia,
        LoadMult,
        MmgdMult,
        PchsMult,
        ColetarVTodasBarras,
        Maxiterations,
        AlowForms,
        SolutionMode,
        PontosASimular,
        FatorCapacidadeMmgd,
        MaxControliterations,
        VminCenarioDivergencia,
        VmaxCenarioDivergencia,
        IMaxCenarioDivergencia,
        AtivarIrrigantes,
        PsoOtimizar,
        TipoOtimizar,
        Oltc,
        Restricao1,
        Restricao2,
        Restricao3,
        Restricao4,
        IncrementoPercentKwUfvs
        ) = args
        
        # Importar dentro da Função 
        # paralelizada
        import py_dss_interface
        dss = py_dss_interface.DSS()
        
        
        # Compila + Configurações
        MyClassDss.Compila(CaminhoDss=CaminhoDss, dss=dss)
        MyClassDss.ConfigLoads(ModeloCarga=ModeloCarga,
                               Limites=[0.85, 1.05],
                               UsarCargasBt=UsarCargasBt,
                               UsarCargasMt=UsarCargasMt,
                               dss=dss,
                               AtivarIrrigantes=AtivarIrrigantes)
        
        MyClassDss.ConfigPchs(UsarPchs=UsarPchsCghs, dss=dss)
        MyClassDss.ConfigCapcontrols(dss=dss)
        TapsCapacitores = MyClassDss.ConfigCapacitors(dss=dss)
        TapsReguladores = MyClassDss.ConfigRegcontrols(dss=dss)
        
        DfCurvaCargas = MyClassDss.CarregaCurvasCargas(DirDss=CaminhoDss)   # Carrega curvas de carga
        DfKwPchs = MyClassDss.CarregaInfoPchs(DirDss=CaminhoDss)            # Carrega as Potência Mensais das Pchs
        DfKwCargas = MyClassDss.CarregaInfoCargas(DirDss=CaminhoDss)        # Carrega as Potência Mensais das Cargas
        
        Losskw, QKvar, Pkw = [],[],[]
        KwMmgd, KwPchs, KvaMmgd, KvaPchs = [],[],[],[]
        ConsumoKw, ConsumoKva = [],[]
        HistVAngBusFase = []
        HistKwKvaBusFaseCargas = []
        HistKwKvaBusFaseMmgd = []
        HistKwKvaBusFasePchs = []
        HistIAngRamosFase = []
        
        Feeder = os.path.basename(os.path.dirname(CaminhoDss)) # Nome do alimentador
        Se = os.path.basename(os.path.dirname(os.path.dirname(CaminhoDss))) # Nome da Subestação
        IndicesOtimizaVColeta = MyClassDss.ExtraiIndices(dss=dss, ColetarVTodasBarras=ColetarVTodasBarras)
        
        
        
        AcharCentroideAlimentador = True
        BarrasCentroide = {}
        # LOOP
        for i in range(PontosASimular):
            [SumKwCargas,
             SumKvarCargas,
             DfCargas] = MyClassDss.CargasUpdate(SimulPoint=i,
                                                 MesIndex=MesIndex,
                                                 DfCurvasCarga=DfCurvaCargas, 
                                                 dss=dss,
                                                 LoadMult=LoadMult, 
                                                 DfKwCargas=DfKwCargas,
                                                 FdIrrigante=FdIrrigante,
                                                 AtivarIrrigantes=AtivarIrrigantes
                                                 )
             
            [SumKwMmgd,
             SumKvarMmgd,
             DfMmgd] = MyClassDss.MmgdUpdate(SimulPoint=i,
                                                 MesIndex=MesIndex,
                                                 dss=dss,
                                                 MmgdMult=MmgdMult, 
                                                 FatorCapacidadeMmgd=FatorCapacidadeMmgd
                                                 )
             
            [SumKwPchs,
             SumKvarPchs,
             DfPchs] = MyClassDss.PchsUpdate(SimulPoint=i,
                                                 MesIndex=MesIndex,
                                                 DfKwPchs=DfKwPchs, 
                                                 dss=dss,
                                                 PchsMult=PchsMult, 
                                                 )
             
            HistKwKvaBusFaseCargas.append(DfCargas)
            HistKwKvaBusFaseMmgd.append(DfMmgd)
            HistKwKvaBusFasePchs.append(DfPchs)
            
            dss.solution.max_iterations = Maxiterations
            dss.solution.max_control_iterations = MaxControliterations
            dss.dssinterface.allow_forms = AlowForms
            dss.solution.mode=SolutionMode
            dss.solution.init_snap()
            dss.solution.solve_plus_control()
            
            MyClassDss.GetTapReguladores(dss=dss, 
                                         TapsReguladores=TapsReguladores)
            
            MyClassDss.GetTapCapacitores(dss=dss, 
                                         TapsCapacitores=TapsCapacitores)
            
            ConsumoKw.append(SumKwCargas)
            ConsumoKva.append(SumKvarCargas)
            KwMmgd.append(SumKwMmgd)
            KvaMmgd.append(SumKvarMmgd)
            KwPchs.append(SumKwPchs)
            KvaPchs.append(SumKvarPchs)
            
            Pkw.append(-1 * round(dss.circuit.total_power[0], 3))          
            QKvar.append(-1 * round(dss.circuit.total_power[1], 3))        
            Losskw.append(round(dss.circuit.line_losses[0], 3))
            #LossK2_verificar = dss.circuit.losses[0]

            if AcharCentroideAlimentador:
                try:
                    BarrasCentroide = MyClassDss.AcharCentroide(Feeder_Path=os.path.dirname(CaminhoDss), dss=dss)
                except Exception as e:
                    print("erro")
                AcharCentroideAlimentador = False
                        
            
            # # Otimizações ======================================================
            # if Otimizar:

            #     Optimizer.ExecutarOtimizacao(dss=dss,
            #                                     Maxiterations=Maxiterations,
            #                                     MaxControliterations=MaxControliterations, 
            #                                     AlowForms=AlowForms,
            #                                     SolutionMode=SolutionMode,
            #                                     PsoOtimizar=PsoOtimizar,
            #                                     TipoOtimizar=TipoOtimizar,
            #                                     Restricao1=Restricao1,
            #                                     Restricao2=Restricao2,
            #                                     Restricao3=Restricao3,
            #                                     Restricao4=Restricao4,
            #                                     IncrementoPercentKwUfvs=IncrementoPercentKwUfvs,
            #                                     Pkw=Pkw,
            #                                     SimulPoint=i,
            #                                     MesIndex=MesIndex, 
            #                                     MmgdMult=MmgdMult, 
            #                                     FatorCapacidadeMmgd=FatorCapacidadeMmgd,
            #                                     SumKwCargas=SumKwCargas,
            #                                     SumKwPchs=SumKwPchs
            #                                     )
            # #===================================================================
            
            
            DfVAngBusFase = MyClassDss.ExtrairVAngBuses(dss=dss, 
                                                        IndicesOtimizaVColeta=IndicesOtimizaVColeta,
                                                        VminCenarioDivergencia=VminCenarioDivergencia, 
                                                        VmaxCenarioDivergencia=VmaxCenarioDivergencia)
        
            DfIAngRamosFase = MyClassDss.ExtrairIAngRamos(dss=dss, 
                                                          IMaxCenarioDivergencia=IMaxCenarioDivergencia)
        
            HistVAngBusFase.append(DfVAngBusFase)
            HistIAngRamosFase.append(DfIAngRamosFase)
            ProgressDict[IndexFeeder] = i + 1
            
            
        # Salvar Dados
        DfDadosRamos = MyClassDss.DadosRamos(dss=dss)        
        DfHistVAngBusFase = pl.concat(HistVAngBusFase)
        DfHistKwKvaBusFaseCargas = pl.concat(HistKwKvaBusFaseCargas)
        DfHistKwKvaBusFaseMmgd =pl.concat(HistKwKvaBusFaseMmgd)
        DfHistKwKvaBusFasePchs = pl.concat(HistKwKvaBusFasePchs)
        DfHistIAngRamosFase = pl.concat(HistIAngRamosFase)
        
        
        [DadosRamo, DadosBarra
        ] = MyClassDss.CondicionamentoDadosSaida(DfHistVAngBusFase, 
                                                DfHistKwKvaBusFaseCargas, 
                                                DfHistKwKvaBusFaseMmgd,
                                                DfHistKwKvaBusFasePchs, 
                                                DfHistIAngRamosFase, 
                                                DfDadosRamos,
                                                i, 
                                                MesIndex,
                                                Feeder,
                                                Se)
            
        MyClassDss.SalvarDados(DadosBarra,
                               DadosRamo,
                               OutputSimul,
                               MesIndex,
                               Feeder,
                               Se)
        
        DfBalanco = MyClassDss.CriarDataFramePowers(ConsumoKw,
                                                    ConsumoKva,
                                                    KwMmgd,
                                                    KwPchs, 
                                                    KvaMmgd,
                                                    KvaPchs,
                                                    Pkw,
                                                    QKvar,
                                                    Losskw)
        
        MyClassDss.SalvarDataframePowers(DfBalanco,
                                         OutputSimul,
                                         MesIndex,
                                         Feeder,
                                         Se)
        
        del HistVAngBusFase
        del HistKwKvaBusFaseCargas
        del HistKwKvaBusFaseMmgd
        del HistKwKvaBusFasePchs
        del HistIAngRamosFase
        del ConsumoKw, ConsumoKva, KwMmgd, KvaMmgd, KwPchs, KvaPchs, Pkw, QKvar, Losskw
        del DfHistVAngBusFase, DfHistIAngRamosFase, DadosRamo, DadosBarra, DfBalanco
        del dss
        gc.collect()


   
    @staticmethod
    def AcharCentroide(Feeder_Path: str, dss: Any) -> dict:
        diretorio_base = os.path.dirname(Feeder_Path) if Feeder_Path.endswith('.dss') else Feeder_Path
        
        arquivo_coords = os.path.join(diretorio_base, 'coords_ctmt.feather')
        CordenadasAlimentadorPlotar = os.path.join(diretorio_base, 'CoordsFormatoPlot.feather')
        SeCoordenadasPlotar = os.path.join(diretorio_base, 'coords_sub.feather')

        if not os.path.exists(arquivo_coords):
            return None

        # 1. Carrega dados
        df_nos = pd.read_feather(arquivo_coords)
        df_nos['pac'] = df_nos['pac'].astype(str).str.lower()
        
        df_trechos = pd.read_feather(CordenadasAlimentadorPlotar) 
        df_sub = pd.read_feather(SeCoordenadasPlotar) 

        # 2. Coleta de Cargas do OpenDSS
        cargas_por_barra = {}
        dss.loads.first()
        for _ in range(dss.loads.count):
            bus_raw = dss.cktelement.bus_names[0]
            bus_limpo = bus_raw.split(".")[0].lower() 
            kw = dss.loads.kw
            
            cargas_por_barra[bus_limpo] = cargas_por_barra.get(bus_limpo, 0) + kw
            dss.loads.next()

        # 3. Mapeia kW para o dataframe de nós
        df_nos['kw'] = df_nos['pac'].map(cargas_por_barra).fillna(0)

        total_kw = df_nos['kw'].sum()
        if total_kw == 0:
            return {"lat": 0, "lon": 0, "total_kw": 0, "barra_proxima": None}

        # 4. Cálculo do Centroide
        lat_cc = (df_nos['lat'] * df_nos['kw']).sum() / total_kw
        lon_cc = (df_nos['lon'] * df_nos['kw']).sum() / total_kw

        distancias = np.sqrt((df_nos['lat'] - lat_cc)**2 + (df_nos['lon'] - lon_cc)**2)
        barra_proxima = df_nos.loc[distancias.idxmin(), 'pac']


        G = MyClassDss.montar_grafo_conectado(df_nos, df_trechos)
        caminho_vermelho = MyClassDss.identificar_caminho_tronco(G, df_nos, df_sub, barra_proxima)

        # IDENTIFICAÇÃO DO RAMO 5/6
        ramo_especifico, d_total = MyClassDss.identificar_ramo_5_6(G, df_nos, caminho_vermelho)

        DictRetorno = {
            "lat": lat_cc, "lon": lon_cc,
            "total_kw": total_kw,
            "barra_proxima": barra_proxima,
            "distancia_centroide": distancias.min(),
            "pacs_tronco": caminho_vermelho,
            "distancia_total_caminho": d_total,
            "ramo_5_6": (ramo_especifico['pac_1'], ramo_especifico['pac_2']) 
        }

        # CHAMADAS DOS PLOTS
        Plot.MapCentroid(df_nos=df_nos, df_trechos=df_trechos, df_sub=df_sub, 
                         DictRetorno=DictRetorno, feederPath=Feeder_Path)

        Plot.PlotCaminhoVermelho(df_nos=df_nos, df_trechos=df_trechos, 
                                 DictRetorno=DictRetorno, feederPath=Feeder_Path)

        return DictRetorno



    @staticmethod
    def montar_grafo_conectado(df_nos, df_trechos):
        """
        Monta o grafo garantindo conectividade total sem gaps, 
        unindo todos os componentes isolados.
        """
        G = nx.Graph()
        
        # 1. Adiciona arestas dos trechos conhecidos
        for _, row in df_trechos.iterrows():
            p1, p2 = str(row['pac_1']).lower(), str(row['pac_2']).lower()
            G.add_edge(p1, p2)

        # 2. Garante que todos os PACs existam como nós
        for pac in df_nos['pac']:
            if not G.has_node(pac):
                G.add_node(pac)

        # 3. Conexão Robusta de Componentes (Gaps)
        # Enquanto o grafo não for um bloco único, conectamos as "ilhas"
        while not nx.is_connected(G):
            componentes = list(nx.connected_components(G))
            # Ordenamos para sempre conectar a menor ilha ao restante do sistema
            componentes.sort(key=len)
            
            comp_isolado = list(componentes[0]) # Menor ilha
            # Todos os outros nós que NÃO estão nessa ilha
            outros_nos = set(G.nodes()) - set(comp_isolado)
            
            df_isolado = df_nos[df_nos['pac'].isin(comp_isolado)]
            df_resto = df_nos[df_nos['pac'].isin(outros_nos)]
            
            # KDTree para encontrar a conexão mais curta possível entre esta ilha e o resto
            tree = cKDTree(df_resto[['lat', 'lon']].values)
            dist, idx = tree.query(df_isolado[['lat', 'lon']].values)
            
            # Encontra o par de nós (um da ilha, um do resto) com a menor distância absoluta
            melhor_par_idx = dist.argmin()
            pac_origem = df_isolado.iloc[melhor_par_idx]['pac']
            pac_destino = df_resto.iloc[idx[melhor_par_idx]]['pac']
            
            # Criamos a "ponte" para fechar o gap
            G.add_edge(pac_origem, pac_destino, weight=dist[melhor_par_idx])
            #print(f"[Gap Fix] Conectando ilha isolada: {pac_origem} -> {pac_destino}")

        return G
    

    @staticmethod
    def identificar_ramo_5_6(G, df_nos, caminho_pacs):
        """
        Calcula a distância total do caminho e identifica o ramo (pac_1, pac_2)
        localizado a aproximadamente 5/6 do percurso saindo da subestação.
        """
        dist_total = 0
        acumulado = []

        # 1. Percorre o caminho calculando a distância euclidiana entre cada PAC
        for i in range(len(caminho_pacs) - 1):
            u, v = caminho_pacs[i], caminho_pacs[i+1]
            n1 = df_nos[df_nos['pac'] == u].iloc[0]
            n2 = df_nos[df_nos['pac'] == v].iloc[0]
            
            # Distância entre barras (aproximada por Pitágoras para coordenadas)
            d = np.sqrt((n1['lat'] - n2['lat'])**2 + (n1['lon'] - n2['lon'])**2)
            dist_total += d
            acumulado.append({'pac_1': u, 'pac_2': v, 'dist_fim': dist_total})

        # 2. Define o alvo de 5/6 da distância total
        alvo_dist = dist_total * (5/6)
        
        # 3. Localiza o ramo específico onde o alvo se encontra
        ramo_selecionado = acumulado[0]
        for trecho in acumulado:
            if trecho['dist_fim'] >= alvo_dist:
                ramo_selecionado = trecho
                break
        
        return ramo_selecionado, dist_total



    @staticmethod
    def identificar_caminho_tronco(G, df_nos, df_sub, barra_destino):
        """Encontra o caminho da subestação até a barra alvo."""
        coords_ext = df_sub['coord_latlon'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
        lat_s, lon_s = coords_ext.apply(lambda x: x[0]).mean(), coords_ext.apply(lambda x: x[1]).mean()
        
        dist_se = np.sqrt((df_nos['lat'] - lat_s)**2 + (df_nos['lon'] - lon_s)**2)
        pac_se = df_nos.loc[dist_se.idxmin(), 'pac']
        return nx.shortest_path(G, source=pac_se, target=barra_destino)

    

    @staticmethod
    def DirsAllDss(DirBase: str,
                   mes_index: int,
                   Resimular: bool) -> list:
        
        
        caminho_script = os.path.abspath(__file__)
        pasta_services = os.path.dirname(caminho_script)
        raiz_projeto = os.path.dirname(pasta_services)
        caminho_final_base = os.path.join(raiz_projeto, DirBase)
        DirBase = os.path.abspath(caminho_final_base)
        
        marcadores_obrigatorios_base = [
            "! ===== SLACK =====",
            "! ===== LINECODES =====",
            "! ===== LINHAS =====",
            "! ===== VOLTAGE BASES ====="
        ]
        marcador_carga_media = "! ===== CARGAS_MEDIA ====="
        marcador_carga_baixa = "! ===== CARGAS_BAIXA ====="

        arquivos_com_tamanho = []
        for subestacao in os.listdir(DirBase):
            caminho_sub = os.path.join(DirBase, subestacao)
            if not os.path.isdir(caminho_sub):
                continue

            for ctmt in os.listdir(caminho_sub):
                caminho_ctmt = os.path.join(caminho_sub, ctmt)
                if not os.path.isdir(caminho_ctmt):
                    continue

                if not Resimular:
                    pasta_dados_gerados = os.path.abspath(os.path.join(DirBase, "..", "DADOS_GERADOS"))
                    caminho_verificacao = os.path.join(pasta_dados_gerados, str(subestacao).strip(), str(ctmt).strip(), str(mes_index))
                    if os.path.exists(os.path.join(caminho_verificacao, "dados_barra.feather")):
                        continue

                dss_arquivo = None
                for f in os.listdir(caminho_ctmt):
                    if f.lower().endswith(".dss"):
                        dss_arquivo = os.path.join(caminho_ctmt, f)
                        break
                
                if not dss_arquivo:
                    continue

                try:
                    with open(dss_arquivo, "r", encoding="utf-8") as f_dss:
                        conteudo = f_dss.read()
                        
                        tem_base = all(m in conteudo for m in marcadores_obrigatorios_base)
                        tem_cargas = (marcador_carga_media in conteudo) or (marcador_carga_baixa in conteudo)
                        
                        if tem_base and tem_cargas:
                            tamanho = os.path.getsize(dss_arquivo)
                            arquivos_com_tamanho.append((dss_arquivo, tamanho))
                except Exception as e:
                    print(f"⚠️ Erro ao processar {dss_arquivo}: {e}")
                    continue

        arquivos_com_tamanho.sort(key=lambda x: x[1], reverse=False)
        lista_temp = [arq[0] for arq in arquivos_com_tamanho]
        lista_final_sequencial = []
        while len(lista_temp) > 1:
            lista_final_sequencial.append(lista_temp.pop(0))  
        
        if lista_temp:
            lista_final_sequencial.append(lista_temp[0])

        return lista_final_sequencial

    @staticmethod
    def SalvarDataframePowers(DfBalanco: pl.DataFrame,
                            OutputSimul: str, 
                            MesIndex: int,
                            Feeder: str,
                            Se: str
                            ) -> None:
        
        caminho_script = os.path.abspath(__file__)
        pasta_services = os.path.dirname(caminho_script)
        raiz_projeto = os.path.dirname(pasta_services)
        
        caminho_output_base = os.path.join(raiz_projeto, OutputSimul)
        if not os.path.exists(caminho_output_base):
            os.makedirs(caminho_output_base, exist_ok=True)
        
        caminho_final = os.path.join(caminho_output_base, Se, Feeder, str(MesIndex))
        
        if not os.path.exists(caminho_final):
            os.makedirs(caminho_final, exist_ok=True)
        
        caminho_feather = os.path.join(caminho_final, "balanco_energetico.feather")
        caminho_xlsx = os.path.join(caminho_final, "balanco_energetico.xlsx")
        
        if DfBalanco is not None and len(DfBalanco) > 0:
            DfBalanco.write_ipc(caminho_feather)
            DfBalanco.write_excel(caminho_xlsx)
            
    @staticmethod
    def CriarDataFramePowers(
                            ConsumoKw,
                            ConsumoKva,
                            KwMmgd,
                            KwPchs, 
                            KvaMmgd,
                            KvaPchs,
                            Pkw,
                            QKvar,
                            Losskw) -> pl.DataFrame:

        # --- BLOCO: CONDICIONAMENTO DE DADOS ---
        P_ajustado = []
        Q_ajustado = []
        Loss_ajustado = []
        
        for i in range(len(Pkw)):
            p_liquido_teorico = ConsumoKw[i] - (KwMmgd[i] + KwPchs[i])
            q_liquido_teorico = ConsumoKva[i] - (KvaMmgd[i] + KvaPchs[i])
            
            # 2. Ajuste de P e Q (Limite de 25% de erro sobre o líquido)
            # P total
            if abs(Pkw[i]) > abs(p_liquido_teorico) * 1.25:
                p_final = p_liquido_teorico
            else:
                p_final = Pkw[i]
                
            # Q total
            if abs(QKvar[i]) > abs(q_liquido_teorico) * 1.25:
                q_final = q_liquido_teorico
            else:
                q_final = QKvar[i]
                
            P_ajustado.append(p_final)
            Q_ajustado.append(q_final)

            # 3. Ajuste de Perdas (Limite de 30% em relação ao P e Q ajustados)
            # Usamos o valor absoluto para garantir que a comparação funcione em fluxo reverso
            limite_perda_p = abs(p_final) * 0.30
            
            if abs(Losskw[i]) > limite_perda_p:
                # Se a perda for irreal, assume-se o limite de 30% ou um valor proporcional
                Loss_ajustado.append(limite_perda_p)
            else:
                Loss_ajustado.append(Losskw[i])
        
        # ---------------------------------------

        dados = {
            "Consumo_P_kW": ConsumoKw,
            "Consumo_Q_kVar": ConsumoKva,
            "Geracao_P_MMGD_kW": KwMmgd,
            "Geracao_P_PCH_kW": KwPchs,
            "Geracao_Q_MMGD_kVar": KvaMmgd,
            "Geracao_Q_PCH_kVar": KvaPchs,
            "P_Total_Circuito_kW": P_ajustado,
            "Q_Total_Circuito_kVar": Q_ajustado,
            "Perdas_Linha_kW": Loss_ajustado
        }
        
        return pl.DataFrame(dados)
                
    @staticmethod
    def SalvarDados(DadosBarra: pl.DataFrame,
                    DadosRamo: pl.DataFrame,
                    OutputSimul: str,
                    MesIndex: int,
                    Feeder: str,
                    Se: str) -> None:
        
        caminho_script = os.path.abspath(__file__)
        pasta_services = os.path.dirname(caminho_script)
        raiz_projeto = os.path.dirname(pasta_services)
        
        caminho_output_base = os.path.join(raiz_projeto, OutputSimul)
        if not os.path.exists(caminho_output_base):
            os.makedirs(caminho_output_base, exist_ok=True)
        
        caminho_final = os.path.join(caminho_output_base, Se, Feeder, str(MesIndex))
        
        if not os.path.exists(caminho_final):
            os.makedirs(caminho_final, exist_ok=True)
        
        caminho_barra = os.path.join(caminho_final, "dados_barra.feather")
        caminho_ramo = os.path.join(caminho_final, "dados_ramo.feather")
            
        if DadosBarra is not None:
            DadosBarra.write_ipc(caminho_barra)
            
        if DadosRamo is not None:
            DadosRamo.write_ipc(caminho_ramo)
            
    @staticmethod
    def CondicionamentoDadosSaida(DfHistVAngBusFase: pl.DataFrame,
                                  DfHistKwKvaBusFaseCargas: pl.DataFrame,
                                  DfHistKwKvaBusFaseMmgd: pl.DataFrame,
                                  DfHistKwKvaBusFasePchs: pl.DataFrame,
                                  DfHistIAngRamosFase: pl.DataFrame,
                                  DfDadosRamos: pl.DataFrame,
                                  i: int, MesIndex: int,
                                  Feeder: str, Se: str) -> tuple[pl.DataFrame, pl.DataFrame]:
        
        
 
        # DADOS DE BARRA ===========================================================================
        i = i + 1 # incremento do passo temporal
        
        # 1 passo: eliminar o .0 de todas as linhas da coluna barra
        
        # 1. Coloque todos os candidatos em uma lista (incluindo o de PCHs)
        candidatos = [
            DfHistVAngBusFase, 
            DfHistKwKvaBusFaseCargas, 
            DfHistKwKvaBusFaseMmgd, 
            DfHistKwKvaBusFasePchs
        ]

        # 2. Processe apenas os que existem e têm conteúdo em uma única linha
        # O 'if df is not None' garante que não haverá erro em variáveis vazias
        lista_processada = [
            df.with_columns(pl.col("barra").str.replace(r"\.0$", "")) 
            if df is not None and len(df) > 0 else df 
            for df in candidatos
        ]

        # 3. Desempacotamento dinâmico
        # Isso substitui todos os seus IFs de verificação de tamanho
        df_V, df_L, df_M, df_P = lista_processada
                    
            
            
        # --- PROCESSAMENTO DE TENSÕES (VERSÃO ULTRA-ROBUSTA) ---
        if df_V is not None:
            #print(f"DEBUG: Processando {df_V.height} linhas. Usando proteção contra Out of Bounds.")
            
            # Criamos as colunas usando null_on_oob=True para evitar o erro de índice
            df_V = df_V.with_columns([
                # FASE A (Sempre tenta pegar o índice 0)
                pl.when(pl.col("barra").str.contains(r"\.1"))
                .then(pl.col("Vpu").list.get(0, null_on_oob=True)).otherwise(None).alias("Va(t)"),
                
                pl.when(pl.col("barra").str.contains(r"\.1"))
                .then(pl.col("Ang").list.get(0, null_on_oob=True)).otherwise(None).alias("Thetaa(t)"),

                # FASE B
                pl.when(pl.col("barra").str.contains(r"\.2")).then(
                    pl.when(pl.col("barra").str.contains(r"\.1"))
                    .then(pl.col("Vpu").list.get(1, null_on_oob=True)) 
                    .otherwise(pl.col("Vpu").list.get(0, null_on_oob=True))
                ).otherwise(None).alias("Vb(t)"),

                pl.when(pl.col("barra").str.contains(r"\.2")).then(
                    pl.when(pl.col("barra").str.contains(r"\.1"))
                    .then(pl.col("Ang").list.get(1, null_on_oob=True))
                    .otherwise(pl.col("Ang").list.get(0, null_on_oob=True))
                ).otherwise(None).alias("Thetab(t)"),

                # FASE C
                pl.when(pl.col("barra").str.contains(r"\.3")).then(
                    pl.when(pl.col("barra").str.contains(r"\.1") & pl.col("barra").str.contains(r"\.2"))
                    .then(pl.col("Vpu").list.get(2, null_on_oob=True))
                    .otherwise(
                        pl.when(pl.col("barra").str.contains(r"\.1") | pl.col("barra").str.contains(r"\.2"))
                        .then(pl.col("Vpu").list.get(1, null_on_oob=True))
                        .otherwise(pl.col("Vpu").list.get(0, null_on_oob=True))
                    )
                ).otherwise(None).alias("Vc(t)"),

                pl.when(pl.col("barra").str.contains(r"\.3")).then(
                    pl.when(pl.col("barra").str.contains(r"\.1") & pl.col("barra").str.contains(r"\.2"))
                    .then(pl.col("Ang").list.get(2, null_on_oob=True))
                    .otherwise(
                        pl.when(pl.col("barra").str.contains(r"\.1") | pl.col("barra").str.contains(r"\.2"))
                        .then(pl.col("Ang").list.get(1, null_on_oob=True))
                        .otherwise(pl.col("Ang").list.get(0, null_on_oob=True))
                    )
                ).otherwise(None).alias("Thetac(t)")
            ])
            
            # Materializa o resultado para garantir que passou
            #print("DEBUG: Finalizado sem erros de índice.")

        # --- PROCESSAMENTO DE POTÊNCIAS (Passos 2 e 5) ---
        def processar_potencia(df):
            if df is not None:
                # 5 passo: Contar fases, dividir e alocar
                df = df.with_columns(
                    # Conta quantas fases existem (ex: .1.2.3 tem 3 fases)
                    n_fases = pl.col("barra").str.count_matches(r"\.\d")
                ).with_columns([
                    (pl.col("Pkw") / pl.col("n_fases")).alias("P_split"),
                    (pl.col("QkVar") / pl.col("n_fases")).alias("Q_split")
                ]).with_columns([
                    # Criando as colunas Pa, Pb, Pc, Qa, Qb, Qc com a divisão
                    pl.when(pl.col("barra").str.contains(r"\.1")).then(pl.col("P_split")).otherwise(0.0).alias(f"Pa(t)"),
                    pl.when(pl.col("barra").str.contains(r"\.2")).then(pl.col("P_split")).otherwise(0.0).alias(f"Pb(t)"),
                    pl.when(pl.col("barra").str.contains(r"\.3")).then(pl.col("P_split")).otherwise(0.0).alias(f"Pc(t)"),
                    pl.when(pl.col("barra").str.contains(r"\.1")).then(pl.col("Q_split")).otherwise(0.0).alias(f"Qa(t)"),
                    pl.when(pl.col("barra").str.contains(r"\.2")).then(pl.col("Q_split")).otherwise(0.0).alias(f"Qb(t)"),
                    pl.when(pl.col("barra").str.contains(r"\.3")).then(pl.col("Q_split")).otherwise(0.0).alias(f"Qc(t)")
                ])
                return df
            return None

        if len(DfHistKwKvaBusFaseCargas) > 0:
            df_L = processar_potencia(df_L)
        if len(DfHistKwKvaBusFaseMmgd) > 0:
            df_M = processar_potencia(df_M)
        if len(DfHistKwKvaBusFasePchs) > 0:
            df_P = processar_potencia(df_P)
        
        
        
        # --- Passo de Limpeza: Mantém apenas 'barra' e as novas colunas temporais ---

        # 1. Para o DataFrame de Tensões
        if df_V is not None:
            df_V = df_V.select([
                pl.col("barra"),
                pl.col(f"Va(t)"),
                pl.col(f"Vb(t)"),
                pl.col(f"Vc(t)"),
                pl.col(f"Thetaa(t)"),
                pl.col(f"Thetab(t)"),
                pl.col(f"Thetac(t)")
            ])

        # 2. Para os DataFrames de Potência (Cargas, MMGD e PCHs)
        def selecionar_colunas_finais(df):
            if df is not None:
                return df.select([
                    pl.col("barra"),
                    pl.col(f"Pa(t)"),
                    pl.col(f"Pb(t)"),
                    pl.col(f"Pc(t)"),
                    pl.col(f"Qa(t)"),
                    pl.col(f"Qb(t)"),
                    pl.col(f"Qc(t)")
                ])
            return None

        if len(DfHistKwKvaBusFaseCargas) > 0:
            df_L = selecionar_colunas_finais(df_L)
        if len(DfHistKwKvaBusFaseMmgd) > 0:
            df_M = selecionar_colunas_finais(df_M)
        if len(DfHistKwKvaBusFasePchs) > 0:
            df_P = selecionar_colunas_finais(df_P)
            
    
        
        # --- BLOCO DE TRATAMENTO DE REPETIÇÕES POR SUB-BLOCOS (i) ---
        
        # O número total de linhas originais por bloco é len(df) / i
        # Vamos processar cada DataFrame para consolidar as barras repetidas em cada intervalo
        
        # 1. Tratamento para Tensões: Mantém apenas a primeira ocorrência da barra no bloco
        if df_V is not None:
            # Criamos um ID de bloco para garantir que a remoção seja feita dentro de cada 'i'
            df_V = (
                df_V.with_row_index("row_nr")
                .with_columns((pl.col("row_nr") // (df_V.height // i)).alias("bloco_id"))
                .group_by(["bloco_id", "barra"], maintain_order=True)
                .first() # Elimina duplicatas mantendo a primeira (tensões são referenciais)
                .drop(["row_nr", "bloco_id"])
            )

        # 2. Tratamento para Potências: Soma as potências se a barra repetir no bloco
        def consolidar_potencias_no_bloco(df, i_passo):
            if df is not None:
                return (
                    df.with_row_index("row_nr")
                    .with_columns((pl.col("row_nr") // (df.height // i_passo)).alias("bloco_id"))
                    .group_by(["bloco_id", "barra"], maintain_order=True)
                    .agg([
                        pl.col(f"Pa(t)").sum(),
                        pl.col(f"Pb(t)").sum(),
                        pl.col(f"Pc(t)").sum(),
                        pl.col(f"Qa(t)").sum(),
                        pl.col(f"Qb(t)").sum(),
                        pl.col(f"Qc(t)").sum()
                    ])
                    .drop("bloco_id")
                )
            return None


            
        if len(DfHistKwKvaBusFaseCargas) > 0:
            df_L = consolidar_potencias_no_bloco(df_L, i)
        if len(DfHistKwKvaBusFaseMmgd) > 0:
            df_M = consolidar_potencias_no_bloco(df_M, i)
        if len(DfHistKwKvaBusFasePchs) > 0:
            df_P = consolidar_potencias_no_bloco(df_P, i)
        
        
        
        # =====================================================================
        # BLOCO DE ARREDONDAMENTO EM MASSA (PROTEÇÃO INTELIGENTE)
        # =====================================================================
        
        # 1. Agrupamos todos os dataframes que PODEM existir
        # Se df_P for None ou vazio, ele simplesmente não entrará na lógica de processamento
        candidatos_arredondar = [df_V, df_L, df_M, df_P]

        # 2. Criamos uma nova lista com os dataframes processados
        # Verificamos individualmente se cada dataframe tem conteúdo antes de arredondar
        dfs_processados = []
        for df in candidatos_arredondar:
            if df is not None and len(df) > 0:
                # Arredonda apenas colunas de ponto flutuante (Float64 e Float32)
                df = df.with_columns(
                    pl.col(pl.Float64, pl.Float32).round(3)
                )
            dfs_processados.append(df)

        # 3. Reatribuição Segura
        # O desempacotamento funciona sempre porque a lista 'dfs_processados' terá 4 itens
        df_V, df_L, df_M, df_P = dfs_processados
        
        
        
        # --- BLOCO DE CONCATENAÇÃO TEMPORAL (LINHAS PARA LISTAS EM COLUNAS) ---
        
        # Para o DataFrame de tensões:
        # Transformamos as múltiplas linhas de cada barra (uma por sub-bloco) 
        # em uma única linha onde cada coluna é uma lista de todos os valores temporais.
        if df_V is not None:
            df_V = (
                df_V.group_by("barra", maintain_order=True)
                .agg([
                    pl.col(f"Va(t)").alias("Va(t)"),
                    pl.col(f"Vb(t)").alias("Vb(t)"),
                    pl.col(f"Vc(t)").alias("Vc(t)"),
                    pl.col(f"Thetaa(t)").alias("Thetaa(t)"),
                    pl.col(f"Thetab(t)").alias("Thetab(t)"),
                    pl.col(f"Thetac(t)").alias("Thetac(t)")
                ])
            )

        # Para os DataFrames de potência (Cargas, MMGD, PCHs):
        # Aplicamos a mesma lógica para que P e Q também virem listas temporais por fase.
        def transformar_em_series_temporais(df):
            if df is not None:
                return (
                    df.group_by("barra", maintain_order=True)
                    .agg([
                        pl.col(f"Pa(t)").alias("Pa(t)"),
                        pl.col(f"Pb(t)").alias("Pb(t)"),
                        pl.col(f"Pc(t)").alias("Pc(t)"),
                        pl.col(f"Qa(t)").alias("Qa(t)"),
                        pl.col(f"Qb(t)").alias("Qb(t)"),
                        pl.col(f"Qc(t)").alias("Qc(t)")
                    ])
                )
            return None



        if len(DfHistKwKvaBusFaseCargas) > 0:
            df_L = transformar_em_series_temporais(df_L)
        if len(DfHistKwKvaBusFaseMmgd) > 0:
            df_M = transformar_em_series_temporais(df_M)
        if len(DfHistKwKvaBusFasePchs) > 0:
            df_P = transformar_em_series_temporais(df_P)
        
        
        
        
        # --- BLOCO DE PADRONIZAÇÃO DE NOMES (INFIXOS) ---
        
        # 1. Renomear colunas do dataframe de MMGD (df_M) adicionando 'fvt'
        if DfHistKwKvaBusFaseMmgd is not None and len(DfHistKwKvaBusFaseMmgd) > 0:
            df_M = df_M.rename({
                "Pa(t)": "Pfvta(t)", "Pb(t)": "Pfvtb(t)", "Pc(t)": "Pfvtc(t)",
                "Qa(t)": "Qfvta(t)", "Qb(t)": "Qfvtb(t)", "Qc(t)": "Qfvtc(t)"
            })

        # 2. Renomear colunas do dataframe de PCHs (df_P) adicionando 'pch'
        if DfHistKwKvaBusFasePchs is not None and len(DfHistKwKvaBusFasePchs) > 0:
            df_P = df_P.rename({
                "Pa(t)": "Ppcha(t)", "Pb(t)": "Ppchb(t)", "Pc(t)": "Ppchc(t)",
                "Qa(t)": "Qpcha(t)", "Qb(t)": "Qpchb(t)", "Qc(t)": "Qpchc(t)"
            })
        
        
        
        # =====================================================================
        # --- BLOCO DE CONSOLIDAÇÃO FINAL (OUTER JOIN AUTOMÁTICO) ---
        # =====================================================================

        # 1. Filtramos apenas os DataFrames que realmente possuem dados e não são None
        # Isso remove a necessidade de verificar se df_P tem linhas manualmente
        lista_final = [d for d in [df_V, df_L, df_M, df_P] if d is not None and len(d) > 0]

        if not lista_final:
            return None # Ou trate o erro caso nenhum dado exista

        # 2. Iniciamos a base com o primeiro DataFrame válido da lista
        dados_barra = lista_final[0]

        # 3. Realizamos o join iterativo para os demais DataFrames presentes
        # O loop percorre do segundo elemento em diante (índice 1:)
        for df_temp in lista_final[1:]:
            # Realizamos o join externo pela coluna 'barra'
            # how="full": Garante que barras presentes em apenas um DF sejam mantidas
            # coalesce=True: Unifica as colunas 'barra' evitando duplicatas como 'barra_right'
            dados_barra = dados_barra.join(
                df_temp, 
                on="barra", 
                how="full", 
                coalesce=True
            )
            
            
        
        # FIM DADOS DE BARRA
        
        # DADOS DE RAMO ============================================================================        

        
         # 1 passo: eliminar o .0 de todas as linhas da coluna barra
        lista_dfs = [
            DfHistIAngRamosFase, DfDadosRamos
        ]
        
        for j in range(len(lista_dfs)):
            if lista_dfs[j] is not None:
                lista_dfs[j] = lista_dfs[j].with_columns(
                    pl.col("ramo").str.replace(r"\.0$", "")
                )

        df_I_din, df_I_est = lista_dfs


        # --- DADOS DE RAMO (CONTINUAÇÃO) ---
        if df_I_din is not None:
            # Alocação correta de fases baseada na string do ramo para Correntes e Ângulos
            # Usamos null_on_oob=True para evitar erros em ramos com menos de 3 fases
            df_I_din = df_I_din.with_columns([
                # FASE A (.1)
                pl.when(pl.col("ramo").str.contains(r"\.1"))
                .then(pl.col("I_amp").list.get(0, null_on_oob=True)).otherwise(None).alias("Ia(t)"),
                
                pl.when(pl.col("ramo").str.contains(r"\.1"))
                .then(pl.col("Ang").list.get(0, null_on_oob=True)).otherwise(None).alias("Thetaa(t)"),

                # FASE B (.2)
                pl.when(pl.col("ramo").str.contains(r"\.2")).then(
                    pl.when(pl.col("ramo").str.contains(r"\.1"))
                    .then(pl.col("I_amp").list.get(1, null_on_oob=True))
                    .otherwise(pl.col("I_amp").list.get(0, null_on_oob=True))
                ).otherwise(None).alias("Ib(t)"),
                
                pl.when(pl.col("ramo").str.contains(r"\.2")).then(
                    pl.when(pl.col("ramo").str.contains(r"\.1"))
                    .then(pl.col("Ang").list.get(1, null_on_oob=True))
                    .otherwise(pl.col("Ang").list.get(0, null_on_oob=True))
                ).otherwise(None).alias("Thetab(t)"),

                # FASE C (.3)
                pl.when(pl.col("ramo").str.contains(r"\.3")).then(
                    pl.when(pl.col("ramo").str.contains(r"\.1") & pl.col("ramo").str.contains(r"\.2"))
                    .then(pl.col("I_amp").list.get(2, null_on_oob=True))
                    .otherwise(
                        pl.when(pl.col("ramo").str.contains(r"\.1") | pl.col("ramo").str.contains(r"\.2"))
                        .then(pl.col("I_amp").list.get(1, null_on_oob=True))
                        .otherwise(pl.col("I_amp").list.get(0, null_on_oob=True))
                    )
                ).otherwise(None).alias("Ic(t)"),
                
                pl.when(pl.col("ramo").str.contains(r"\.3")).then(
                    pl.when(pl.col("ramo").str.contains(r"\.1") & pl.col("ramo").str.contains(r"\.2"))
                    .then(pl.col("Ang").list.get(2, null_on_oob=True))
                    .otherwise(
                        pl.when(pl.col("ramo").str.contains(r"\.1") | pl.col("ramo").str.contains(r"\.2"))
                        .then(pl.col("Ang").list.get(1, null_on_oob=True))
                        .otherwise(pl.col("Ang").list.get(0, null_on_oob=True))
                    )
                ).otherwise(None).alias("Thetac(t)")
            ])
            
            
            if df_I_din is not None:
                # 1. Alocação inicial das fases (Ainda como floats individuais)
                # Aqui você executa aquele bloco pl.when(pl.col("ramo").str.contains(r"\.1"))...
                df_I_din = df_I_din.with_columns([
                    # ... (seu código de extração de Ia, Ib, Ic, etc.)
                ])


                # ==========================================================
                # 4. TRATAMENTO DE REPETIÇÕES E AGREGAÇÃO FINAL
                # ==========================================================
                # Tratamento de sub-blocos
                df_I_din = (
                    df_I_din.with_row_index("row_nr")
                    .with_columns((pl.col("row_nr") // (df_I_din.height // i)).alias("bloco_id"))
                    .group_by(["bloco_id", "ramo"], maintain_order=True)
                    .first()
                    .drop(["row_nr", "bloco_id"])
                )

                # Agora sim transformamos em LISTA para o formato final
                df_I_din = (
                    df_I_din.with_columns(pl.col(pl.Float64, pl.Float32).round(3))
                    .group_by("ramo", maintain_order=True)
                    .agg([
                        pl.col("Ia(t)"), pl.col("Ib(t)"), pl.col("Ic(t)"),
                        pl.col("Thetaa(t)"), pl.col("Thetab(t)"), pl.col("Thetac(t)")
                    ])
                )
            
            
        # Consolidação Final de Ramos
        if df_I_din is not None and df_I_est is not None:
            dados_ramo = df_I_est.join(df_I_din, on="ramo", how="full", coalesce=True)
        else:
            dados_ramo = df_I_din if df_I_din is not None else df_I_est
            
            
            
        # --- BLOCO DE ADIÇÃO DE METADADOS (MES, ALIMENTADOR, SE) ---
        
        # 1. Mapeamento do nome do mês (considerando que mes_index começa em 0)
        nomes_meses = [
            "Janeiro", "Fevereiro", "Março", "Abril", "Maio", "Junho",
            "Julho", "Agosto", "Setembro", "Outubro", "Novembro", "Dezembro"
        ]
        mes_extenso = nomes_meses[MesIndex]

        # 2. Adição das colunas em dados_barra
        if dados_barra is not None:
            dados_barra = dados_barra.with_columns([
                pl.lit(mes_extenso).alias("Mes"),
                pl.lit(Feeder).alias("Alimentador"),
                pl.lit(Se).alias("Subestacao")
            ])

        # 3. Adição das colunas em dados_ramo
        if dados_ramo is not None:
            dados_ramo = dados_ramo.with_columns([
                pl.lit(mes_extenso).alias("Mes"),
                pl.lit(Feeder).alias("Alimentador"),
                pl.lit(Se).alias("Subestacao")
            ])
            
           
        return dados_barra, dados_ramo
         
    @staticmethod
    def DadosRamos(dss: Any) -> None:
        
        Nome = []
        INom = []
        IEmerg = []
        Metros = []
        
        dss.lines.first()
        for _ in range(dss.lines.count):
            nome = dss.lines.name
            bus1 = dss.lines.bus1
            partes = bus1.split(".")
            fases = ".".join(partes[1:]) if len(partes) > 1 else ""
            
            completo = str(nome) + str('.') + str(fases)
            I_nom = dss.lines.norm_amps
            I_emerg = dss.lines.emerg_amps
            metros = dss.lines.length
            
            Nome.append(completo)
            INom.append(I_nom)
            IEmerg.append(I_emerg)
            Metros.append(metros)
            
            dss.lines.next()
        
        # 3. Criar DataFrame Longo
        df_final = pl.DataFrame({"ramo": Nome,
                                 "I_nom": INom,
                                 "I_emerg": IEmerg,
                                 "metros": Metros})
        
        
        # 7. Seleção final com arredondamento
        df_final.select([
            pl.col("ramo"),pl.col("I_nom"),
            pl.col("I_emerg"),pl.col("metros").round(3)
        ])
        
        return df_final
        
    @staticmethod
    def ExtrairIAngRamos(dss: Any, IMaxCenarioDivergencia: float) -> pl.DataFrame:
        # 1. Pré-contagem para estimar tamanho (opcional) ou usar listas simples por coluna
        # Listas de tipos primitivos (floats/strings) são mais leves que listas de dicionários
        nomes = []
        fases = []
        correntes = []
        angulos = []

        dss.lines.first()
        for _ in range(dss.lines.count):
            nome = dss.lines.name
            bus1 = dss.lines.bus1
            num_fases = dss.cktelement.num_phases
            
            # Identificação das fases
            partes = bus1.split(".")
            lista_fases = partes[1:] if len(partes) > 1 else [str(i+1) for i in range(num_fases)]
            
            # Extração direta do buffer de memória do OpenDSS
            c_raw = dss.cktelement.currents_mag_ang
            
            # Adiciona apenas os dados do terminal 1 (entrada da linha)
            for i in range(num_fases):
                nomes.append(nome)
                fases.append(lista_fases[i])
                correntes.append(c_raw[i * 2])     # Magnitude
                angulos.append(c_raw[i * 2 + 1]) # Ângulo
                
            dss.lines.next()

        # 2. Criação do DataFrame a partir de colunas (muito mais eficiente)
        df_bruto = pl.DataFrame({
            "ramo": nomes,
            "fase_num": fases,
            "I": correntes,
            "Ang": angulos
        })

        # --- LÓGICA DE FILTRAGEM E MÉDIA ---
        # Cálculo da média ignorando outliers
        media_valida = (
            df_bruto
            .filter((pl.col("I") >= 0) & (pl.col("I") <= IMaxCenarioDivergencia))
            .select(pl.col("I").mean())
            .item()
        )

        df_final = (
            df_bruto
            .with_columns([
                pl.when((pl.col("I") >= 0) & (pl.col("I") <= IMaxCenarioDivergencia))
                .then(pl.col("I"))
                .otherwise(media_valida)
                .alias("I")
            ])
            .group_by("ramo", maintain_order=True)
            .agg([
                pl.col("fase_num").sort().str.concat(".").alias("sufixo"),
                pl.col("I").alias("I_amp"),
                pl.col("Ang").alias("Ang")
            ])
            .select([
                (pl.col("ramo") + "." + pl.col("sufixo")).alias("ramo"),
                pl.col("I_amp"),
                pl.col("Ang")
            ])
        )

        return df_final
    
    @staticmethod
    def ExtrairVAngBuses(dss: Any, IndicesOtimizaVColeta: list,
                         VminCenarioDivergencia: float,
                         VmaxCenarioDivergencia: float ) -> pl.DataFrame:
        
        # 1. Extração rápida via vetores
        v_pu = np.array(dss.circuit.buses_vmag_pu)
        v_raw = np.array(dss.circuit.buses_volts)
        nomes_nos = dss.circuit.nodes_names 
        angulos = np.degrees(np.arctan2(v_raw[1::2], v_raw[0::2]))

        # 2. Criar DataFrame Longo
        df_long = pl.DataFrame({
            "no_bruto": nomes_nos,
            "Vpu": v_pu,
            "Ang": angulos
        })
        
        # --- NOVO BLOCO DE FILTRO/SUBSTITUIÇÃO ---
        # Se Vpu < 0.8 ou Vpu > 1.25, substitui por 1.0
        df_long = df_long.with_columns(
            pl.when((pl.col("Vpu") < VminCenarioDivergencia) | (pl.col("Vpu") > VmaxCenarioDivergencia))
            .then(1.0)
            .otherwise(pl.col("Vpu"))
            .alias("Vpu")
        )
        # ----------------------------------------

        # 3. Aplicar Filtro
        if IndicesOtimizaVColeta is not None:
            df_long = df_long[IndicesOtimizaVColeta]

        # 4. Transformação Corrigida
        df_final = (
            df_long
            .with_columns(
                # Garante que pegamos apenas o que vem antes do primeiro ponto como barra
                pl.col("no_bruto").str.splitn(".", 2).struct.rename_fields(["barra", "fase"]).alias("campos")
            )
            .unnest("campos")
            .group_by("barra", maintain_order=True)
            .agg([
                # CORREÇÃO AQUI: unique() remove as repetições e sort() mantém a ordem 1.2.3
                pl.col("fase").unique().sort().str.concat("."),
                pl.col("Vpu").unique().sort(),
                pl.col("Ang").unique().sort()
            ])
            .with_columns(
                # Monta o nome final sem duplicatas
                (pl.col("barra") + "." + pl.col("fase")).alias("barra")
            )
            .select([
                pl.col("barra"),
                pl.col("Vpu"),
                pl.col("Ang")
            ])
        )

        return df_final
            
    @staticmethod
    def GetTapCapacitores(dss: Any, TapsCapacitores: dict) -> None:
            
        dss.capacitors.first()
        for _ in range(dss.capacitors.count):
            nome = dss.capacitors.name 
            tap = dss.capacitors.states
            if len(tap) == 0.0:
                valor_decimal = sum(tap) / 1
            else:
                valor_decimal = sum(tap) / len(tap)
            TapsCapacitores[nome].append(valor_decimal)
            dss.capacitors.next()
                    
    @staticmethod
    def GetTapReguladores(dss: Any, TapsReguladores: dict) -> None:
        
        dss.regcontrols.first()
        for _ in range(dss.regcontrols.count):
            nome = dss.regcontrols.name
            tap = dss.regcontrols.tap_number
            TapsReguladores[nome].append(tap)
            dss.regcontrols.next()
        
    @staticmethod
    def CurvaIrradiance(fator_capacidade_desejado: float,
                                                dias: int) -> list:
        
        A = 1.0           # Pico de irradiação (máximo 1)
        mu = 12.5         # Horário do pico (12:30h - tipicamente o meio-dia solar)
        N_PONTOS_DIA = 96 # 24 horas * 4 pontos/hora (15 em 15 min)
        LIMITE_MINIMO = 0.0001 # Valor mínimo para representar a noite
        
        # Constante de conversão (Integral da Gaussiana normalizada: sqrt(2*pi) approx 2.5066)
        SQRT_2PI = 2.50663
        HORAS_NO_DIA = 24

        # --- 2. CÁLCULO DA LARGURA (SIGMA) BASEADO NO FC ---
        # FC = (Energia Gerada) / (Capacidade Máxima * Horas Totais)
        # Energia Gerada ~ Integral(Gaussiana) = A * sqrt(2*pi) * sigma
        # Horas de Sol Pleno (HSP) = FC * 24 horas
        
        HSP_desejada = fator_capacidade_desejado * HORAS_NO_DIA
        
        # Ajustando sigma: sigma = HSP / (A * sqrt(2*pi))
        sigma = HSP_desejada / (A * SQRT_2PI)

        # --- 3. GERAÇÃO DA CURVA DE 96 PONTOS (1 DIA) ---
        tempo_pontos = np.linspace(1, HORAS_NO_DIA, N_PONTOS_DIA, endpoint=False) 

        # Função Gaussiana
        irradiacao_pu = A * np.exp(-0.5 * ((tempo_pontos - mu) / sigma)**2)

        # Aplica o limite mínimo
        irradiacao_pu[irradiacao_pu < LIMITE_MINIMO] = LIMITE_MINIMO

        # Lista base de 96 pontos
        curva_irradiacao_96 = np.round(irradiacao_pu, 4).tolist()

        # --- 4. EXTRAPOLAÇÃO ---
        irradiacao_extrapolada = curva_irradiacao_96 * dias
        
        return irradiacao_extrapolada
    
    @staticmethod
    def PchsUpdate(SimulPoint: int,
                MesIndex: int,
                DfKwPchs: pl.DataFrame, 
                dss: Any,
                PchsMult: float, 
                ) -> tuple[float, float, pl.DataFrame]:

        """ Atualiza a potência das PCHs de acordo com o mês usando DataFrames Polars """

        indice_mes = max(1, min(12, MesIndex)) - 1
        sum_p = 0.0
        sum_q = 0.0
        
        list_barra = []
        list_pkw = []
        list_qkw = []
        
        if SimulPoint == 0:  
            dss.generators.first()
            for _ in range(dss.generators.count):
                nome = dss.generators.name
                row_pch = DfKwPchs.filter(pl.col("cod_id") == nome)
                
                if not row_pch.is_empty():
                    pot_mes = row_pch.select(pl.col("curva").list.get(indice_mes)).item()
                    dss.generators.kw = pot_mes * PchsMult
                
                dss.generators.next()

        dss.generators.first()
        for _ in range(dss.generators.count):
            bus_com_fases = dss.cktelement.bus_names[0]
            p_atual = dss.generators.kw
            q_atual = 0.426 * p_atual
            list_barra.append(bus_com_fases)
            list_pkw.append(p_atual)
            list_qkw.append(q_atual)
            
            sum_p += p_atual 
            sum_q += q_atual
            
            dss.generators.next()
            
        df_pchs = pl.DataFrame({
            "barra": list_barra,
            "Pkw": list_pkw,
            "QkVar": list_qkw
        })

        return sum_p, sum_q, df_pchs

    @staticmethod
    def MmgdUpdate(SimulPoint: int,
                   MesIndex: int,
                   dss: Any,
                   MmgdMult: float, 
                   FatorCapacidadeMmgd: float
                   ) -> None:
        
        sum_p = 0.0
        sum_q = 0.0
        Days = 3
        # Inicialização de listas: Bufers colunares
        list_barra = []
        list_pkw = []
        list_qkw = []

        # Altera a irradiação em cima do painel - aumenta/diminui
        curva_irradiancia = MyClassDss.CurvaIrradiance(fator_capacidade_desejado=FatorCapacidadeMmgd,
                                                                     dias=Days)
        irradi_now = curva_irradiancia[SimulPoint] 

        # Altera a capacidade de geração - aumenta/diminui
        if SimulPoint == 0:
            dss.pvsystems.first()
            for _ in range(dss.pvsystems.count):
                dss.pvsystems.kva = dss.pvsystems.kva * MmgdMult
                dss.pvsystems.pmpp = dss.pvsystems.pmpp * MmgdMult
                dss.pvsystems.next()
    

        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            dss.pvsystems.irradiance = irradi_now
            bus_com_fases = dss.cktelement.bus_names[0]
            P_kw = dss.pvsystems.kva * irradi_now 
            # fp = 0.92
            Q_kvar = 0.426 * P_kw
            # Armazenamento nos buffers
            list_barra.append(bus_com_fases)
            list_pkw.append(P_kw)
            list_qkw.append(Q_kvar)
            
            # Sem fluxo de potência: Apenas para conferir resultados
            sum_p += P_kw 
            sum_q += Q_kvar
            
            dss.pvsystems.next()
    
        df_mmgd = pl.DataFrame()
        if dss.pvsystems.count > 0:
            # Criação do dataframe Polars
            df_mmgd = pl.DataFrame({"barra": list_barra,
                                    "Pkw": list_pkw,
                                    "QkVar": list_qkw}).with_columns([
                                        pl.col("Pkw"),
                                        pl.col("QkVar")])
        return sum_p, sum_q, df_mmgd
    
    @staticmethod
    def CargasUpdate(SimulPoint: int, MesIndex: int, 
                    DfCurvasCarga: pl.DataFrame, dss: Any, 
                    LoadMult: float, DfKwCargas: pl.DataFrame,
                    FdIrrigante: float,
                    AtivarIrrigantes: bool) -> tuple[float, float, pl.DataFrame]:
        """Atualiza as cargas no OpenDSS conforme a estrutura do DataFrame."""
        
        # 1. Determina o tipo de dia e o índice baseado no SimulPoint (0-287)
        if SimulPoint <= 95:
            tip_dia, idx_t = "DU", SimulPoint
        elif SimulPoint < 192:
            tip_dia, idx_t = "DO", SimulPoint - 96
        else:
            tip_dia, idx_t = "SA", SimulPoint - 192

        # Buffers para o DataFrame de retorno
        list_barra, list_pkw, list_qkw = [], [], []
        sum_p, sum_q = 0.0, 0.0
        
        # 2. Loop pelas cargas ativas no circuito
        dss.loads.first()
        for _ in range(dss.loads.count):
            name = dss.loads.name
            bus_com_fases = dss.cktelement.bus_names[0]

            # 3. Busca a potência e o tipo de carga (tip_cc) no DfKwCargas
            # Ajustado para usar 'cod_id' conforme sua imagem
            row_uc = DfKwCargas.filter(pl.col("cod_id") == name)
            
            if row_uc.is_empty():
                dss.loads.next()
                continue
                
            # Extrai o tipo de carga para buscar a curva correspondente
            tip_cc = row_uc.select("tip_cc").item()
            
            # Pega a potência mensal (ajuste se a potência também for uma lista em 'curva')
            # Se no seu DfKwCargas a potência estiver na coluna 'curva' por mês:
            mes_kw = row_uc.select(pl.col("curva").list.get(MesIndex - 1)).item()

            # 4. Busca o multiplicador na DfCurvasCarga
            # Ajustado: sua imagem mostra as colunas 'tip_cc' e 'curva' neste DF também
            mult_ponto = DfCurvasCarga.filter(
                (pl.col("crvcrg_cod_id") == tip_cc) & (pl.col("tip_dia") == tip_dia)
            ).select(pl.col("curva").list.get(idx_t)).item()

            # 5. Cálculo da potência final
            p_final = mes_kw * mult_ponto * LoadMult
            
            # Aplica fator de demanda se for irrigante
            if name.lower().startswith("irrigante"):
                if not AtivarIrrigantes:
                    p_final = 0
                p_final *= FdIrrigante
                
            # 6. Atualiza o OpenDSS diretamente
            dss.loads.kw = p_final
            q_final = 0.426 * p_final # FP fixo 0.92 (Q = P * tan(acos(0.92)))
            
            # Armazena nos buffers
            list_barra.append(bus_com_fases)
            list_pkw.append(round(p_final, 4))
            list_qkw.append(round(q_final, 4))
            
            sum_p += p_final
            sum_q += q_final
            
            dss.loads.next()

        # Cria o DataFrame de saída
        df_cargas = pl.DataFrame({
            "barra": list_barra,
            "Pkw": list_pkw,
            "QkVar": list_qkw
        })
        
        return sum_p, sum_q, df_cargas
    
    @staticmethod
    def Compila(CaminhoDss: str, dss: Any) -> None:
        """Compila o DSS"""
        dss.text("Clear")
        dss.text("Set DefaultBaseFrequency=60")
        dss.text(f'Compile "{CaminhoDss}"')
        
    @staticmethod
    def ConfigLoads(ModeloCarga: int, Limites: list,
                    UsarCargasBt: bool, UsarCargasMt: bool
                    ,dss: Any,
                    AtivarIrrigantes: bool) -> None:
        """ 1: P constante e Q constante (padrão): comumente usados para estudos de fluxo de potência
            2: Z constante (ou impedância constante)
            3: P constante e Q quadrático
            4: Exponencial:
            5: I constante (ou magnitude de corrente constante) Às vezes usado para carga retificadora
            6: P constante e Q fixo (no valor nominal)
            7: P constante e Q quadrático (ou seja, reatância fixa)
            8: CEP (ver ZIPV)"""
            
        vmin = Limites[0]                                           
        vmax = Limites[1]
        dss.loads.first()
        for _ in range(dss.loads.count):
            NomeCarga = dss.loads.name
            
            if NomeCarga.lower().startswith("irrigante"):
                if AtivarIrrigantes == False:
                    dss.cktelement.enabled(0)
                    
            # Modelo da carga
            dss.loads.model = ModeloCarga
            # Limites para Z cte
            dss.loads.vmax_pu = vmax
            dss.loads.vmin_pu = vmin
            
            if not UsarCargasBt:
                kv = float(dss.loads.kv)
                if kv < 1.0:
                    dss.cktelement.enabled(0)
                    
            if not UsarCargasMt:
                kv = float(dss.loads.kv)
                if kv > 1.0:
                    dss.cktelement.enabled(0)
            
            dss.loads.next()
        
    @staticmethod
    def ConfigMmgd(UsarMmgdBt: bool, UsarMmgdMt: bool,
                   dss: Any) -> None:
        
        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            NomePv = dss.pvsystems.name
            
            if not UsarMmgdBt:
                kv = float(dss.pvsystems.kv)
                if kv < 1.0:
                    dss.cktelement.enabled(0)
                    
            if not UsarMmgdMt:
                kv = float(dss.pvsystems.kv)
                if kv > 1.0:
                    dss.cktelement.enabled(0)
            
            dss.pvsystems.next()
            
    @staticmethod
    def ConfigPchs(UsarPchs: bool, dss: Any) -> None:
        
        dss.generators.first()
        for _ in range(dss.generators.count):
            if not UsarPchs:
                dss.cktelement.enabled(0)
            dss.generators.next()
            
    @staticmethod
    def ConfigCapacitors(dss: Any) -> dict:
        TapsCapacitors = {}
        dss.capacitors.first()
        for _ in range(dss.capacitors.count):
            dss.capacitors.states = [1,1,0,0,0,0,0,0]
            TapsCapacitors[dss.capacitors.name] = []
            dss.capacitors.next()
        return TapsCapacitors
        
    @staticmethod
    def ConfigCapcontrols(dss: Any) -> None:
        TempoAtuacao = 1
        dss.capcontrols.first()
        for _ in range(dss.capcontrols.count):
            dss.capcontrols.delay = TempoAtuacao
            TempoAtuacao += 1
            dss.capcontrols.next()
            
    @staticmethod
    def ConfigRegcontrols(dss: Any) -> dict:
        TapsRegcontrols = {}
        for nome in dss.regcontrols.names:
            TapsRegcontrols[nome] = []
            dss.regcontrols.next()
        return TapsRegcontrols
            
    @staticmethod
    def CarregaCurvasCargas(DirDss: str) -> pl.DataFrame:
        
        DirDss = Path(DirDss)
        ArqCurvas = next(DirDss.parent.glob("[Cc]urvas*feather"), None)
        
        if not ArqCurvas: return pl.DataFrame()
        
        # Carrega o arquivo e padroniza as colunas em minusculo
        df = pl.read_ipc(ArqCurvas, memory_map=False).rename(lambda c: c.lower())
        
        # Identificação das Colunas de potência
        PotCols = [c for c in df.columns if c.startswith("pot_")]
        
        df = df.with_columns(
            curva=pl.concat_list(pl.col(PotCols)).list.eval(pl.element().round(4))
        ).select(["crvcrg_cod_id", "tip_dia", "curva"])
        
        return df
    
    @staticmethod
    def  CarregaInfoPchs(DirDss: str) -> pl.DataFrame:
        
        DirDss = Path(DirDss)
        ArqPchs = next(DirDss.parent.glob("[Pp]chs*feather"), None)
        
        if not ArqPchs: return pl.DataFrame()
        
        # Carrega o arquivo e padroniza as colunas em minusculo
        df = pl.read_ipc(ArqPchs, memory_map=False).rename(lambda c: c.lower())
        
        # Identificação das Colunas de potência
        PotCols = [c for c in df.columns if c.startswith("potencia_mes_")]
        
        df = df.with_columns(
            curva=pl.concat_list(pl.col(PotCols)).list.eval(pl.element().round(4))
        ).select(["cod_id", "curva"])
        
        return df
    
    @staticmethod
    def  CarregaInfoCargas(DirDss: str) -> pl.DataFrame:
        
        DirDss = Path(DirDss)
        ArqPchs = next(DirDss.parent.glob("[Uu]cs*feather"), None)
        
        if not ArqPchs: return pl.DataFrame()
        
        # Carrega o arquivo e padroniza as colunas em minusculo
        df = pl.read_ipc(ArqPchs, memory_map=False).rename(lambda c: c.lower())
        
        # Identificação das Colunas de potência
        PotCols = [c for c in df.columns if c.startswith("potencia_mes_")]
        
        df = df.with_columns(
            curva=pl.concat_list(pl.col(PotCols)).list.eval(pl.element().round(4))
        ).select(["cod_id","tip_cc", "curva"])
        
        return df
    
    @staticmethod
    def ExtraiIndices(dss: str, ColetarVTodasBarras: bool) -> list:
        
        Indices = []
        if ColetarVTodasBarras:
            IndicesCargas = MyClassDss.ExtraiIndicesCargas(dss=dss)
            IndicesMmgd = MyClassDss.ExtraiIndicesMmgd(dss=dss)
            IndicesPchs = MyClassDss.ExtraiIndicesPchs(dss=dss)
            Indices = IndicesCargas + IndicesMmgd + IndicesPchs

        return Indices
     
    @staticmethod
    def ExtraiIndicesCargas(dss: str) -> list:
        
        # 1. Coleta os nomes das barras das cargas
        buses_loads_raw = []
        dss.loads.first()
        for _ in range(dss.loads.count):
            bus = dss.cktelement.bus_names[0].lower()
            buses_loads_raw.append(bus)
            dss.loads.next()
            
        # 2. Processamento com Polars para limpar e expandir (Explodir fases)
        df_buses = pl.DataFrame({"barra_raw": buses_loads_raw})

        df_cargas_expandido = (
            df_buses
            .with_columns(
                pl.col("barra_raw").str.replace(r"\.0$", "") # Remove o .0 final
            )
            .with_columns(
                pl.col("barra_raw").str.split(".").alias("partes")
            )
            .with_columns(
                pl.col("partes").list.get(0).alias("base"),
                pl.col("partes").list.slice(1).alias("fases")
            )
            .explode("fases") 
            .select(
                (pl.col("base") + "." + pl.col("fases")).alias("barra_fase")
            )
        )

       # 3. Coleta a lista global de todos os nós (nó = barra.fase)
        buses_todos = [n.lower() for n in dss.circuit.nodes_names]

        # Correção: Criar o DataFrame garantindo que o tamanho da lista e dos índices coincidam
        df_todos = pl.DataFrame({
            "no_global": buses_todos
        }).with_row_index(name="idx_global") # Método mais seguro para gerar índices no Polars
        
        
        # 4. Cruzamento (JOIN) para encontrar os índices
        # Cruzamos a lista de cargas expandida com a lista global de nós
        df_resultado = df_cargas_expandido.join(
            df_todos, 
            left_on="barra_fase", 
            right_on="no_global", 
            how="inner"
        )

        # 5. Guardar os índices e os nomes encontrados
        indices_cargas = df_resultado["idx_global"].to_list()
        #nomes_finais = df_resultado["barra_fase"].to_list()

        return indices_cargas
    
    @staticmethod
    def ExtraiIndicesMmgd(dss: str) -> list:
        
        # 1. Coleta os nomes das barras das cargas
        buses_mmgd_raw = []
        indices_mmgd = []
        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            bus = dss.cktelement.bus_names[0].lower()
            buses_mmgd_raw.append(bus)
            dss.pvsystems.next()
            
        if buses_mmgd_raw:
            # 2. Processamento com Polars para limpar e expandir (Explodir fases)
            df_buses = pl.DataFrame({"barra_raw": buses_mmgd_raw})

            df_mmgd_expandido = (
                df_buses
                .with_columns(
                    pl.col("barra_raw").str.replace(r"\.0$", "") # Remove o .0 final
                )
                .with_columns(
                    pl.col("barra_raw").str.split(".").alias("partes")
                )
                .with_columns(
                    pl.col("partes").list.get(0).alias("base"),
                    pl.col("partes").list.slice(1).alias("fases")
                )
                .explode("fases") 
                .select(
                    (pl.col("base") + "." + pl.col("fases")).alias("barra_fase")
                )
            )

        # 3. Coleta a lista global de todos os nós (nó = barra.fase)
            buses_todos = [n.lower() for n in dss.circuit.nodes_names]

            # Correção: Criar o DataFrame garantindo que o tamanho da lista e dos índices coincidam
            df_todos = pl.DataFrame({
                "no_global": buses_todos
            }).with_row_index(name="idx_global") # Método mais seguro para gerar índices no Polars
            
            
            # 4. Cruzamento (JOIN) para encontrar os índices
            # Cruzamos a lista de cargas expandida com a lista global de nós
            df_resultado = df_mmgd_expandido.join(
                df_todos, 
                left_on="barra_fase", 
                right_on="no_global", 
                how="inner"
            )

            # 5. Guardar os índices e os nomes encontrados
            indices_mmgd = df_resultado["idx_global"].to_list()
            #nomes_finais = df_resultado["barra_fase"].to_list()

        return indices_mmgd
    
    @staticmethod
    def ExtraiIndicesPchs(dss: str) -> list:
                 
        # 1. Coleta os nomes das barras das cargas
        buses_pchs_raw = []
        indices_pchs = []
        dss.generators.first()
        for _ in range(dss.generators.count):
            bus = dss.cktelement.bus_names[0].lower()
            buses_pchs_raw.append(bus)
            dss.generators.next()
            
        
        if buses_pchs_raw:
            # 2. Processamento com Polars para limpar e expandir (Explodir fases)
            df_buses = pl.DataFrame({"barra_raw": buses_pchs_raw})

            df_pchs_expandido = (
                df_buses
                .with_columns(
                    pl.col("barra_raw").str.replace(r"\.0$", "") # Remove o .0 final
                )
                .with_columns(
                    pl.col("barra_raw").str.split(".").alias("partes")
                )
                .with_columns(
                    pl.col("partes").list.get(0).alias("base"),
                    pl.col("partes").list.slice(1).alias("fases")
                )
                .explode("fases") 
                .select(
                    (pl.col("base") + "." + pl.col("fases")).alias("barra_fase")
                )
            )

        # 3. Coleta a lista global de todos os nós (nó = barra.fase)
            buses_todos = [n.lower() for n in dss.circuit.nodes_names]

            # Correção: Criar o DataFrame garantindo que o tamanho da lista e dos índices coincidam
            df_todos = pl.DataFrame({
                "no_global": buses_todos
            }).with_row_index(name="idx_global") # Método mais seguro para gerar índices no Polars
            
            
            # 4. Cruzamento (JOIN) para encontrar os índices
            # Cruzamos a lista de cargas expandida com a lista global de nós
            df_resultado = df_pchs_expandido.join(
                df_todos, 
                left_on="barra_fase", 
                right_on="no_global", 
                how="inner"
            )

            # 5. Guardar os índices e os nomes encontrados
            indices_pchs = df_resultado["idx_global"].to_list()
            #nomes_finais = df_resultado["barra_fase"].to_list()

        return indices_pchs