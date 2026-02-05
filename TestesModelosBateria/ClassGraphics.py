import matplotlib.pyplot as plt
import numpy as np

class Graphics:
    def __init__(self, p_carga_lista, p_rede_lista, v_min_lista, horas, soc_2, kwh_lista, kwh_listav2):
        self.p_carga_lista = p_carga_lista
        self.p_rede_lista = p_rede_lista
        self.v_min_lista = v_min_lista
        self.horas = horas
        self.soc_2 = soc_2
        self.kwh_lista = kwh_lista
        self.kwh_listav2 = kwh_listav2
    
    def PlotSimulacao(self):
        fig, (ax1, ax2, ax3, ax4) = plt.subplots(4, 1, figsize=(12, 16), sharex=True)

        # --- SUBPLOT 1: POTÊNCIAS ---
        # Trocado .step por .plot para criar linhas interpoladas
        # --- SUBPLOT 1: POTÊNCIAS ---
        ax1.plot(self.horas, self.p_carga_lista, color='gray', label='BESS (kW)', linewidth=1, zorder=5)
        ax1.plot(self.horas, self.p_rede_lista, color='black', label='Subestação (Rede)', linewidth=1, zorder=6)
        
        h_array = np.array(self.horas)
        p_array = np.array(self.p_carga_lista)

        # VERMELHO: Quando a bateria CARREGA (p_array < 0)
        # Isso acontecerá no fluxo reverso se a bateria estiver absorvendo a energia
        ax1.fill_between(h_array, p_array, 0, 
                         where=(p_array < 0),
                         color='red', alpha=0.5, label='Charge (Fluxo Reverso)', 
                         interpolate=True, zorder=2)

        # VERDE: Quando a bateria DESCARREGA (p_array > 0)
        ax1.fill_between(h_array, p_array, 0, 
                         where=(p_array > 0),
                         color='lime', alpha=0.5, label='Discharge', 
                         interpolate=True, zorder=2)

        ax1.set_ylabel('Potência [kW]')
        ax1.set_title('Simulação BESS com Linha e Carga Adicional (Interpolado)')
        ax1.yaxis.grid(True, alpha=0.2)
        ax1.axhline(0, color='black', linewidth=1, zorder=1)
        ax1.legend(loc='upper right')

        # --- SUBPLOTS 2, 3 e 4 (LINHAS SUAVES) ---
        
        # Subplot 2: Energia
        ax2.plot(self.horas, self.kwh_lista, color='Black', label='Energia (kWh)', linewidth=1)
        ax2.set_ylabel('Energia [kWh]')
        ax2.yaxis.grid(True, alpha=0.2)
        ax2.legend(loc='upper right')
        
        # Subplot 2: Energia
        ax2.plot(self.horas, self.kwh_listav2, color='brown', label='Energia V2 (kWh)', linewidth=1)
        ax2.set_ylabel('Energia [kWh]')
        ax2.yaxis.grid(True, alpha=0.2)
        ax2.legend(loc='upper right')

        
        # Subplot 3: SOC 2
        ax3.plot(self.horas, self.soc_2, color='brown', label='SOC v2(%)', linewidth=1)
        ax3.set_ylabel('SOC [%]')
        ax3.set_ylim(-5, 105)
        ax3.yaxis.grid(True, alpha=0.2)
        ax3.legend(loc='upper right')

        # Subplot 4: Tensão
        ax4.plot(self.horas, self.v_min_lista, color='Black', label='V_min (pu)', linewidth=1)
        ax4.set_ylabel('Tensão Min [pu]')
        ax4.set_xlabel('Tempo [h]')
        ax4.yaxis.grid(True, alpha=0.2)
        ax4.legend(loc='upper right')

        plt.xticks(range(0, 25, 2))
        plt.tight_layout()
        plt.show()