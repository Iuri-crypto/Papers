import numpy as np
from typing import Any
import pyswarms as ps
import matplotlib.pyplot as plt
import polars as pl

        
class Optimizer:
    
    cache_metricas = {"drp": [], "drc": [], "losses": []}
    
    @staticmethod
    def ObterHistorico(optimizer):
        return np.array(optimizer.pos_history)
    
    @staticmethod
    def Solver(Maxiterations,
                            MaxControliterations,
                            AlowForms,
                            SolutionMode,
                            dss):
        dss.solution.max_iterations = Maxiterations
        dss.solution.max_control_iterations = MaxControliterations
        dss.dssinterface.allow_forms = AlowForms
        dss.solution.mode = SolutionMode
        dss.solution.init_snap()
        dss.solution.solve_plus_control()
        
    @staticmethod
    def Objetivo(Tap: float,
                 dss,
                 Maxiterations,
                 MaxControliterations, 
                 AlowForms,
                 SolutionMode,
                 BaseKv,
                 BasePu) -> float:
        
        tap_discreto = round(float(Tap), 3)
        dss.vsources.base_kv = BaseKv * (1 + tap_discreto * 0.00625)
        dss.vsources.pu = BasePu * (1 + tap_discreto * 0.00625)
        
        Optimizer.Solver(Maxiterations, MaxControliterations, AlowForms, SolutionMode, dss)
        
        V = np.array(dss.circuit.buses_vmag_pu)
        TotalLeituras = len(V)
        
        # Cálculos de métricas
        n_prec = np.sum(((V >= 0.93) & (V < 0.95)) | ((V > 1.05) & (V <= 1.07)))
        n_crit = np.sum((V < 0.92) | (V > 1.07))
        
        Drp = (n_prec / TotalLeituras) * 100
        Drc = (n_crit / TotalLeituras) * 100
        Loss = dss.circuit.line_losses[0]

        # ARMAZENANDO NO CACHE (Para o gráfico posterior)
        Optimizer.cache_metricas["drp"].append(Drp)
        Optimizer.cache_metricas["drc"].append(Drc)
        Optimizer.cache_metricas["losses"].append(Loss)
        
        CustoTotal = Drp * 100 + Drc * 1000 + Loss * 100
        return CustoTotal

    @staticmethod
    def PlotarConvergencia(historico):
        plt.figure(figsize=(10,6))
        for i in range(historico.shape[1]):
            plt.plot(historico[:, i, 0], alpha=0.5)
        plt.xlabel("Iteração")
        plt.ylabel("Tap")
        plt.grid(True)
        plt.show()
         
    @staticmethod
    def PlotarMetricasEvolucao(n_particulas,
                               iters):
        """
        Gera os plots usando o cache da otimização:
        - Enxame: Cinza claro ao fundo.
        - Melhor Solução: Preto grosso por cima de todos.
        """
        # Organiza os dados capturados durante o f_pso
        drp_mat = np.array(Optimizer.cache_metricas["drp"]).reshape(iters, n_particulas)
        drc_mat = np.array(Optimizer.cache_metricas["drc"]).reshape(iters, n_particulas)
        loss_mat = np.array(Optimizer.cache_metricas["losses"]).reshape(iters, n_particulas)

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)
        
        # 1. Plotando o enxame (Cinza claro)
        for p in range(n_particulas):
            # Usamos alpha baixo para criar o efeito de "nuvem"
            ax1.plot(drp_mat[:, p], color='lightgray', alpha=0.5, linewidth=2, zorder=1)
            ax1.plot(drc_mat[:, p], color='lightgray', alpha=0.5, linewidth=2, zorder=1)
            ax2.plot(loss_mat[:, p], color='lightgray', alpha=0.5, linewidth=2, zorder=1)
        
        # 2. Plotando a Melhor Solução (Preto Grosso)
        # zorder=10 garante que fique acima de qualquer outra linha ou grade
        ax1.plot(np.min(drp_mat, axis=1), label='Melhor DRP da Iteração', 
                 color='black', linewidth=1.4, zorder=10)
        ax1.plot(np.min(drc_mat, axis=1), label='Melhor DRC da Iteração', 
                 color='black', linestyle='--', linewidth=1.4, zorder=10)
        
        ax2.plot(np.min(loss_mat, axis=1), label='Melhor Perda Global', 
                 color='black', linewidth=1.4, zorder=10)

        # Formatação Estética
        ax1.set_ylabel("Violação de Tensão (%)")
        ax1.set_title("Evolução da Qualidade de Tensão (Enxame vs. Melhor)")
        ax1.legend(loc='upper right')
        ax1.grid(True, linestyle=':', alpha=0.6)

        ax2.set_ylabel("Perdas Totais (W)")
        ax2.set_xlabel("Iteração")
        ax2.set_title("Evolução das Perdas (Enxame vs. Melhor)")
        ax2.legend(loc='upper right')
        ax2.grid(True, linestyle=':', alpha=0.6)

        plt.tight_layout()
        plt.show()
        
    @staticmethod
    def f_pso(particulas,
              **kwargs):
        return [Optimizer.Objetivo(Tap=p[0], **kwargs) for p in particulas]
                    
    @staticmethod
    def ExtractKvaUfvs(dss: Any,
                       Dic: dict):
        
        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            Nome = dss.pvsystems.name
            kva = dss.pvsystems.kva
            pmpp = dss.pvsystems.pmpp
            
            if Nome not in Dic:
                Dic[Nome] = {}
            Dic[Nome]['kva'] = round(kva, 4)
            Dic[Nome]['pmpp'] = round(pmpp, 4)
            dss.pvsystems.next()
            
        return Dic
    
    @staticmethod
    def ZerarPowersUfvs(dss: Any):
        dss.pvsystems.first()
        for _ in range(dss.pvsystems.count):
            dss.pvsystems.kva = 0
            dss.pvsystems.pmpp = 0
            dss.pvsystems.next()
            
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
    def MmgdUpdate(SimulPoint: int,
                   MesIndex: int,
                   dss: Any,
                   MmgdMult: float, 
                   FatorCapacidadeMmgd: float,
                   Multiplicador: float,
                   DictKvasPmppsUfvsCasoBase: dict
                   ) -> None:
        
        sum_p = 0.0
        sum_q = 0.0
        Days = 3
        list_barra = []
        list_pkw = []
        list_qkw = []

        # Altera a irradiação em cima do painel - aumenta/diminui
        curva_irradiancia = Optimizer.CurvaIrradiance(fator_capacidade_desejado=FatorCapacidadeMmgd,
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
            Nome = dss.pvsystems.name
            
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
    def CargasDados(dss: Any) -> tuple[float, float, pl.DataFrame]:
       
        list_barra = []
        list_pkw = []
        list_qkva = []
        list_kv = []
        dss.loads.first()
        for _ in range(dss.loads.count):
            bus_com_fases = dss.cktelement.bus_names[0]
            kw = dss.loads.kw
            kva = dss.loads.kva
            kv = dss.loads.kv
            list_barra.append(bus_com_fases)
            list_pkw.append(round(kw, 4))
            list_qkva.append(round(kva, 4))
            list_kv.append(round(kv, 4))
            
            dss.loads.next()

        df_cargas = pl.DataFrame({
            "barra": list_barra,
            "Pkw": list_pkw,
            "QkVar": list_qkva,
            "kv": list_kv
        })
        
        return df_cargas
    
    @staticmethod
    def InserirUfvsBarrasPq(dss: Any, df_cargas: pl.DataFrame):
        
        cont=0
        dss.loads.first()
        for _ in range(dss.loads.count):
            bus_com_fases = (dss.cktelement.bus_names[0].split("."))
            QuantFases = [p for p in bus_com_fases[1:] if p != '0']
            kv = dss.loads.kv
            
            dss.text(f"New xycurve.mypvst_{cont} npts = 4 xarray=[0 25 75 100] yarray=[1.2 1.0 0.8 0.6]")
            dss.text(f"New loadshape.myirrad_{cont} npts = 1 interval = 1 mult = [1] ")
            dss.text(f"New tshape.mytemp_{cont} npts = 1 interval = 1 temp = [25]")
            dss.text(f"New pvsystem.{cont} Vminpu=0.5 Vmaxpu=1.5 phases = {QuantFases} conn = wye bus1 = {bus_com_fases}")
            dss.text(f"~ kv = {kv} kva = {0} pmpp = {0}")
            dss.text(f"~ pf = 1 %cutin = 0.00005 %cutout = 0.00005 varfollowinverter = Yes effcurve = myeff_{cont}")
            dss.text(f"~ p-tcurve = mypvst_{cont} daily = myirrad_{cont} tdaily = mytemp_{cont}")
            
            cont+=1
            dss.loads.next()
            

        
    
    @staticmethod
    def ExecutarOtimizacao(dss: Any,
                            Maxiterations: int,
                            MaxControliterations: int, 
                            AlowForms: int,
                            SolutionMode: int,
                            PsoOtimizar: bool,
                            TipoOtimizar: str,
                            Restricao1: str,
                            Restricao2: str,
                            Restricao3: str,
                            Restricao4: str,
                            IncrementoPercentKwUfvs: float,
                            Pkw: list,
                            SimulPoint: int,
                            MesIndex: int,
                            MmgdMult, 
                            FatorCapacidadeMmgd,
                            SumKwCargas,
                            SumKwPchs
                            ):
        
       
        
        if TipoOtimizar == "HC_MMGD":
            Multiplicador=0
            incrementos = list()
            for _ in range(10000):
                incrementos.append(round(Multiplicador))
                Multiplicador += IncrementoPercentKwUfvs
            
            
            # Salva potências antes de zerar (Pode ser usado no futuro para análises comparativas!!!)
            DictKvasPmppsUfvsCasoBase = Optimizer.ExtractKvaUfvs(dss, Dic={})
            
            # 1- zerar as potências dos ufvs existentes no alimentador
            Optimizer.ZerarPowersUfvs(dss=dss)
            
            # 2- Inserir Ufvs nas barras PQ
            df_cargas = Optimizer.CargasDados(dss=dss)
            Optimizer.InserirUfvsBarrasPq(dss=dss, 
                                          df_cargas=df_cargas)
            
            
            
            Optimizer.cache_metricas = {"drp": [], "drc": [], "losses": []}
            
            BaseKv = dss.vsources.base_kv
            BasePu = dss.vsources.pu
            n_particulas = 50
            iters = 15
            
            
            ConfiguracsaoAtivosOtimizar = []
            
            dss.capacitors.first()
            for _ in range(dss.capacitors.count):
                ConfiguracsaoAtivosOtimizar.append({
                    "Ativo": "Capacitor",
                    "Nome": dss.capacitors.name,
                    "LimiteInferior": 0,
                    "LimiteSuperior": 1,
                    "Otimizar": "tap"
                })
                
            dss.regcontrols.first()
            for _ in range(dss.regcontrols.count):
                ConfiguracsaoAtivosOtimizar.append({
                    "Ativo": "Regulador",
                    "Nome": dss.regcontrols.name,
                    "LimiteInferior": -16,
                    "LimiteSuperior": 16,
                    "Otimizar": "Estagio"
                })
                
            ConfiguracsaoAtivosOtimizar.append({
                "Ativo": "OLTC",
                "Nome": "OLTC",
                "LimiteInferior": -16,
                "LimiteSuperior": 16,
                "Otimizar": "tap"
            })
            
            ConfiguracsaoAtivosOtimizar.append({
                "Ativo": "BESS",
                "Nome": "BESS",
                "LimiteInferior": 0,
                "LimiteSuperior": 10000,
                "Otimizar": "Pkw"
            })
            
            dimensoes = len(ConfiguracsaoAtivosOtimizar)
            
            min_bounds = np.array([config["LimiteInferior"] for config in ConfiguracsaoAtivosOtimizar])
            max_bounds = np.array([config["LimiteSuperior"] for config in ConfiguracsaoAtivosOtimizar])
            bounds = (min_bounds, max_bounds)
            
            options = {'c1': 0.5, 'c2': 0.85, 'w': 0.25}

            

            optimizer = ps.single.GlobalBestPSO(
                                                n_particles=n_particulas, 
                                                dimensions=dimensoes, 
                                                options=options, 
                                                bounds=bounds
                                            )
            
            optimizer = ps.single.GlobalBestPSO(n_particles=n_particulas,
                                                dimensions=dimensoes,
                                                options=options,
                                                bounds=bounds)
            
            custo, BestTap = optimizer.optimize(Optimizer.f_pso,
                                                iters=iters,
                                                dss=dss,
                                                Maxiterations=Maxiterations,
                                                MaxControliterations=MaxControliterations,
                                                AlowForms=AlowForms,
                                                SolutionMode=SolutionMode,
                                                BaseKv=BaseKv,
                                                BasePu=BasePu
                                            )
            
            Optimizer.PlotarMetricasEvolucao(n_particulas,
                                                iters)
            
            Historico = Optimizer.ObterHistorico(optimizer=optimizer)
            
            Optimizer.PlotarConvergencia(historico=Historico)
            
            return BestTap, Optimizer.ObterHistorico(optimizer)