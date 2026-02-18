from dataclasses import dataclass

@dataclass
class SimConfig:
    # Mês 
    MesIndex: int = 9
    
    # Modelagem
    modelo_carga: int = 1
    usar_cargas_bt: bool = True
    usar_cargas_mt: bool = True
    usar_mmgd_bt: bool = True
    usar_mmgd_mt: bool = True
    usar_pchs_cghs: bool = True
    
    # Parâmetros técnicos
    fd_irrigante: float = 0.75
    Oltc: bool = True
    
    # parâmetros fluxo
    ErroFluxoTolerancia: float = 0.001
    LoadMult: float = 1.0
    MmgdMult: float = 1.0
    PchsMult: float = 1.0
    ColetarVTodasBarras: bool = True
    Maxiterations: int = 100
    MaxControliterations: int = 50
    AlowForms: int = 0
    SolutionMode: int = 0
    PontosASimular: int = 288
    FatorCapacidadeMmgd: float = 0.23
    AtivarIrrigantes: bool = False
    
    # Filtragem de possíveis divergências
    VminCenarioDivergencia: float = 0.75
    VmaxCenarioDivergencia: float = 1.25
    IMaxCenarioDivergencia: float = 10000
    
    # Número de Processos
    NumWorkers: int = 1
    
    # Caminhos
    DirBase: str = r"feeders"
    OutputSimul: str = r"output"
    
    # PSO
    PsoOtimizar = False
    Cenario = "HC_MMGD"
    kwpPackage = 7.5
    RodarCenarioBase = True # Sem otimização
    Restricao1 = {"Vmax": 1.5}
    Restricao2 = 'FLUXO_REVERSO'
    Restricao3 = 'SOBRECARGA'
    Restricao4 = 'PERDAS'
    IncrementoPercentKwUfvs = float(5)
    TapMinOltc = 0.9
    TapMaxOlts = 1.1
    sobregeracao = 300 
    
    # Infos Reguladores de Tensão
    #================================
    TapMaxAVR = 1.1
    TapMinAVR = 0.9
    QuantidadeAVR = 2
    DistAVR = [0.9, 0.6] # Distância do Regulador  em relação a subestação até o baricentro de carga
    xhl = 1
    LoadLoss = 0.5
    r = 0.0001
    x = 0.0001
    ptratio = 15
    band = 2
    #================================
    
    # Infos Bancos de Capacitores
    #================================
    QuantidadeBkShunt = 2
    DistBkShunt = [0.95, 0.8] # Distância do Banco de Capacitor  em relação a subestação até o baricentro de carga
    Numsteps = 8
    FatorPotenciaAlvo = 0.96
    #================================

    