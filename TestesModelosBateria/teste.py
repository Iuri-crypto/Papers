from ClassGraphics import Graphics
import py_dss_interface
dss = py_dss_interface.DSS()

dss.text("Clear")
dss.text("New Circuit.Source bus1=A basekv=0.48 phases=3 pu=1.0")
dss.text("New Linecode.LC1 nphases=3 R1=0.02794 X1=0.02502 units=km")
dss.text("New Line.Linha1 bus1=A bus2=B linecode=LC1 length=10 units=km")

dss.text("New Load.CargaFixa phases=3 bus1=B kv=0.48 kw=10 conn=wye model=1")

dss.text(f"New Load.CargaBateria phases=3 bus1=B kv=0.48 kw=0 conn=wye model=1")

dss.text("Set voltagebases=[0.48]")
dss.text("Calcvoltagebases")
dss.text("Set mode=Snapshot")

horas = list(range(25)) 
p_carga_lista = []
p_rede_lista = []

# Parâmetros Nominais
kw_rated = 35.0
kwh_rated = 500.0
soc_atual = 50.0  
kwcargaFixa = 30

energia_atual = (soc_atual / 100.0) * kwh_rated 

soc_lista = [soc_atual] 
kwh_lista = [energia_atual]
v_min_lista = [] 

loadshapeCargafixa = [
    0.18, 0.15, -0.13, -0.12, 0.12, 0.15,
    0.35, 0.55, 0.40, 0.35, 0.30, 0.25, 
    0.22, 0.22, -0.25, -0.30, -0.45, 0.65, 
    0.85, 1.00, 0.95, 0.80, 0.55, 0.30  
]



for h in range(24):
    p_simulada = 0.0
    
    dss.loads.first()
    for _ in range(dss.loads.count):
        Nome = dss.loads.name
        if Nome.startswith("cargabateria"):
            
            if loadshapeCargafixa[h] > 0.3:
                p_simulada = kw_rated * loadshapeCargafixa[h]
                dss.loads.kw = -1*kw_rated * loadshapeCargafixa[h]

            elif loadshapeCargafixa[h] <= 0:
                p_simulada = -1 * kw_rated * 0.4
                dss.loads.kw =  kw_rated  * 0.4
                
            else:
                dss.loads.kw = 0
        
        elif Nome.startswith("cargafixa"):
            dss.loads.kw = kwcargaFixa * loadshapeCargafixa[h]

        dss.loads.next()

    dss.solution.solve()
    
    # 3. Captura do menor valor de tensão (pu) em cada ponto simulado
    v_min_lista.append(min(dss.circuit.buses_vmag_pu))

    # Coleta de dados de Potência
    dss.circuit.set_active_element("Vsource.Source")
    p_rede = -sum(dss.cktelement.powers[0:6:2]) 
    
    p_carga_lista.append(p_simulada)
    p_rede_lista.append(p_rede)

    # Integração Manual do SOC
    energia_atual += (-1*p_simulada * 1.0)
    energia_atual = max(0, min(kwh_rated, energia_atual))
    soc_atual = (energia_atual / kwh_rated) * 100.0
    
    soc_lista.append(soc_atual)
    kwh_lista.append(energia_atual)

# Ajuste de sincronismo visual
p_carga_lista.append(p_carga_lista[-1])
p_rede_lista.append(p_rede_lista[-1])
v_min_lista.append(v_min_lista[-1])

# Plot
graphics = Graphics(p_carga_lista,
                    p_rede_lista,
                    v_min_lista,
                    horas,
                    soc_lista,
                    kwh_lista)

graphics.PlotSimulacao()





# from ClassGraphics import Graphics
# import py_dss_interface

# dss = py_dss_interface.DSS()

# # --- CONFIGURAÇÃO DO SISTEMA ---
# dss.text("Clear")
# dss.text("New Circuit.Source bus1=A basekv=0.48 phases=3 pu=1.0")
# dss.text("New Linecode.LC1 nphases=3 R1=0.02794 X1=0.02502 units=km")
# dss.text("New Line.Linha1 bus1=A bus2=B linecode=LC1 length=10 units=km")

# # Carga Fixa
# dss.text("New Load.CargaFixa phases=3 bus1=B kv=0.48 kw=10 conn=wye model=1")

# # --- DEFINIÇÃO DO STORAGE (Usando Propriedades Oficiais da Documentação) ---
# # kWhrated: Capacidade nominal
# # kWrated: Potência nominal do inversor
# # %stored: Estado de carga inicial (0-100)
# # %EffCharge: Eficiência de carga
# dss.text(f"New Storage.Bateria phases=3 bus1=B kv=0.48 kWrated=50 kWhrated=500 %stored=100 %reserve=20 %EffCharge=91 %EffDischarge=91 State=IDLING")

# dss.text("Set voltagebases=[0.48]")
# dss.text("Calcvoltagebases")
# #dss.text("Set mode=Snapshot")

# dss.text("Set mode=Daily") # Mude para Daily para habilitar a progressão de tempo
# dss.text("Set stepsize=1h") # Define que cada passo dura 1 hora
# dss.text("Set number=1")    # Resolve 1 passo por vez

# # --- LISTAS E PARÂMETROS ---
# horas = list(range(25)) 
# p_carga_lista = []
# p_rede_lista = []
# soc_lista = [] 
# kwh_lista = []
# v_min_lista = [] 

# kwcargaFixa = 50
# loadshapeCargafixa = [
#     0.65, 0.60, 0.58, 0.55, 0.55, 0.60, # Madrugada
#     0.70, 0.65, -0.50, 0.35, 0.25, 0.20, # Manhã (Aumento solar)
#     0.18, 0.15, 0.18, 0.25, 0.40, 0.60, # Tarde (Pico solar)
#     0.85, 1.00, 0.95, 0.85, 0.75, 0.70  # Noite (Pico de demanda)
# ]

# # --- LOOP DE SIMULAÇÃO ---
# for h in range(24):
#     mult = loadshapeCargafixa[h]
    
#     # 1. Atualiza Carga Fixa
#     dss.loads.first()
#     for _ in range(dss.loads.count):
#         nome = dss.loads.name
#         if nome == 'cargafixa':
#             dss.loads.kw = kwcargaFixa * mult
#         dss.loads.next()
    
#     # 2. Gerenciamento do Storage
#     # State: {IDLING | CHARGING | DISCHARGING}
#     # --- GERENCIAMENTO DO STORAGE ---
#     dss.storages.first()
#     for _ in range(dss.storages.count):
#         if mult > 0.8:
#             # PICO: Descarga para a rede (Fica POSITIVO no gráfico)
#             dss.text(f"Edit Storage.Bateria State=DISCHARGING %Discharge={abs(mult) * 100}")
#         elif mult < 0.3: 
#             # FLUXO REVERSO ou CARGA BAIXA: Carrega a bateria (Fica NEGATIVO no gráfico)
#             dss.text(f"Edit Storage.Bateria State=CHARGING %Charge={abs(mult) * 100}")
#         else:
#             # ESTADO DE ESPERA: Não carrega nem descarrega
#             dss.text(f"Edit Storage.Bateria State=IDLING")
#         dss.storages.next()

#     # 3. Resolve o fluxo de potência
#     dss.solution.solve()

#     # 4. Captura de Dados direto do Elemento
#     dss.circuit.set_active_element("Storage.Bateria")
#     # Powers retorna Potência Ativa (kW) e Reativa (kvar).
#     # No modo DISCHARGING, kW é positivo (injetando). No CHARGING, kW é negativo (consumindo).
#     p_barramento = sum(dss.cktelement.powers[0:6:2])
    
#     # Invertemos para o seu gráfico: Positivo=Injeção/Descarga, Negativo=Consumo/Carga
#     p_carga_lista.append(-1 * p_barramento)

#     # 5. Captura das Propriedades de Energia (Nomes exatos da Documentação)
#     # %stored: % de kWhrated
#     # kWhstored: Energia atual em kWh
#     # O OpenDSS aplica %EffCharge internamente no kWhstored
#     soc_lista.append(float(dss.text("? Storage.Bateria.%stored")))
#     kwh_lista.append(float(dss.text("? Storage.Bateria.kWhstored")))

#     # 6. Tensão e Rede
#     v_min_lista.append(min(dss.circuit.buses_vmag_pu))
#     dss.circuit.set_active_element("Vsource.Source")
#     p_rede_lista.append(-sum(dss.cktelement.powers[0:6:2]))

# # --- AJUSTES FINAIS E PLOT ---
# p_carga_lista.append(p_carga_lista[-1])
# p_rede_lista.append(p_rede_lista[-1])
# soc_lista.append(soc_lista[-1])
# kwh_lista.append(kwh_lista[-1])
# v_min_lista.append(v_min_lista[-1])

# graphics = Graphics(p_carga_lista, p_rede_lista, v_min_lista, horas, soc_lista, kwh_lista)
# graphics.PlotSimulacao()