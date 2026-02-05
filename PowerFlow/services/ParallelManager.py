import time
import os
from concurrent.futures import ProcessPoolExecutor
from multiprocessing import Manager
from services.DSSMyFuntions import MyClassDss

class ParallelManager:
    def __init__(self, config, monitor):
        self.cfg = config
        self.monitor = monitor
        
    def executar(self, arquivosDSS: list):
        """ Faz a execução paralela e coordena com o monitor"""
        total_files = len(arquivosDSS)
        nomes_alimentadores = [os.path.basename(os.path.dirname(c))
                               for c in arquivosDSS]
        
        with Manager() as manager:
            # Dicionario partilhado entre processos para o progresso
            progreso_dict = manager.dict({i: 0 for i in range(total_files)})
            
            # Preparação dos argumentos para o MyClassDSS
            args_list = [
                (
                    CaminhoDss,                      # Variável local do loop
                    self.cfg.OutputSimul,           # Caminho
                    self.cfg.MesIndex,              # Mês
                    self.cfg.modelo_carga,          # Modelagem
                    self.cfg.usar_cargas_bt,        # Modelagem
                    self.cfg.usar_cargas_mt,        # Modelagem
                    self.cfg.usar_mmgd_bt,          # Modelagem
                    self.cfg.usar_mmgd_mt,          # Modelagem
                    self.cfg.usar_pchs_cghs,        # Modelagem
                    IndexFeeder,                    # Variável local do loop
                    progreso_dict,                  # Dicionário de progresso (Manager)
                    self.cfg.fd_irrigante,          # Parâmetros técnicos
                    self.cfg.otimizar,              # Parâmetros técnicos
                    self.cfg.ErroFluxoTolerancia,   # Parâmetros fluxo
                    self.cfg.LoadMult,              # Parâmetros fluxo
                    self.cfg.MmgdMult,              # Parâmetros fluxo
                    self.cfg.PchsMult,              # Parâmetros fluxo
                    self.cfg.ColetarVTodasBarras,   # Parâmetros fluxo
                    self.cfg.Maxiterations,         # Parâmetros fluxo
                    self.cfg.AlowForms,             # Parâmetros fluxo
                    self.cfg.SolutionMode,          # Parâmetros fluxo
                    self.cfg.PontosASimular,        # Parâmetros fluxo
                    self.cfg.FatorCapacidadeMmgd,   # Parâmetros fluxo
                    self.cfg.MaxControliterations,  # Parâmetros fluxo
                    self.cfg.VminCenarioDivergencia, # Filtragem
                    self.cfg.VmaxCenarioDivergencia, # Filtragem
                    self.cfg.IMaxCenarioDivergencia,  # Filtragem
                    self.cfg.AtivarIrrigantes,
                    self.cfg.PsoOtimizar,
                    self.cfg.TipoOtimizar,
                    self.cfg.Oltc,
                    self.cfg.Restricao1,
                    self.cfg.Restricao2,
                    self.cfg.Restricao3,
                    self.cfg.Restricao4,
                    self.cfg.IncrementoPercentKwUfvs
                )
                
                for IndexFeeder, CaminhoDss in enumerate(arquivosDSS)]
            
            # Inicio do Pool de Processos
            with ProcessPoolExecutor(max_workers=self.cfg.NumWorkers) as executor:
                # Dispara os processos em background
                executor.map(MyClassDss.RunFeeder, args_list)
                
                # Loop de controle da Interface (na thread principal)
                while True:
                    concluidos = self.monitor.AtualizarTela(
                        progreso_dict,
                        total_files,
                        nomes_alimentadores,
                        self.cfg.PontosASimular
                    )
                    
                    if concluidos == total_files:
                        break
                    time.sleep(1)