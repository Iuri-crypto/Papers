"""
Autor: Iuri Lorenzo Quirino Moraes Silva
20/01/2026
"""
import sys
import os
from services.config import SimConfig
from services.SimulationMonitor import SimulationMonitor
from services.DSSMyFuntions import MyClassDss
from services.ParallelManager import ParallelManager


dir_lib = os.path.join(os.path.dirname(__file__), 'lib')
if dir_lib not in sys.path: sys.path.insert(0, dir_lib)

def main():
    # Instancia as configurações
    config = SimConfig()
     
    # 1. List de Diretórios dos arquivos DSS
    DssFilesDir = MyClassDss.DirsAllDss(DirBase=config.DirBase,
                                        mes_index=config.MesIndex,
                                        Resimular=True)
    
    # 2. Inicializar os componentes  
    monitor = SimulationMonitor(num_workers=config.NumWorkers)
    manager = ParallelManager(config=config, monitor=monitor)
    
    # 3. Rodar os processos
    manager.executar(arquivosDSS=DssFilesDir)
    
if __name__ == '__main__':
    main()