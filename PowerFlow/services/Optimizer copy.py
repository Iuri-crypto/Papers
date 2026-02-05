import numpy as np
from typing import Any
import pyswarms as ps
import matplotlib.pyplot as plt

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
        
        # Forçando o Tap a ser discreto (opcional, mas recomendado para OLTC)
        tap_discreto = round(float(Tap), 3)
        
        BaseKv = BaseKv * 1.1
        BasePu = BasePu * 1.1
        
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
    def ExecutarOtimizacao(dss: Any,
                            Oltc: bool,
                            Maxiterations: int,
                            MaxControliterations: int, 
                            AlowForms: int,
                            SolutionMode: int):
        
        if Oltc:
            Optimizer.cache_metricas = {"drp": [], "drc": [], "losses": []}
            
            BaseKv = dss.vsources.base_kv
            BasePu = dss.vsources.pu
            n_particulas = 50
            iters = 15
            dimensoes = 1
            
            min_bounds = np.ones(dimensoes) * -16
            max_bounds = np.ones(dimensoes) * 16
            bounds = (min_bounds, max_bounds)
            
            options = {'c1': 0.5, 'c2': 0.85, 'w': 0.25}

            def f_pso(particulas):
                return [Optimizer.Objetivo(
                                                Tap=p[0], 
                                                dss=dss,
                                                Maxiterations=Maxiterations,
                                                MaxControliterations=MaxControliterations,
                                                AlowForms=AlowForms,
                                                SolutionMode=SolutionMode,
                                                BaseKv=BaseKv,
                                                BasePu=BasePu
                                            ) for p in particulas]

            optimizer = ps.single.GlobalBestPSO(
                n_particles=n_particulas, 
                dimensions=dimensoes, 
                options=options, 
                bounds=bounds
            )
            
            optimizer = ps.single.GlobalBestPSO(n_particles=n_particulas, dimensions=1, options=options, bounds=bounds)
            
            custo, BestTap = optimizer.optimize(f_pso, iters=iters, verbose=True)
            
            Optimizer.PlotarMetricasEvolucao(n_particulas, iters)
            
            Historico = Optimizer.ObterHistorico(optimizer=optimizer)
            
            Optimizer.PlotarConvergencia(historico=Historico)
            
            return BestTap, Optimizer.ObterHistorico(optimizer)