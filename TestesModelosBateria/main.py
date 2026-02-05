from ClassGraphics import Graphics
from ClassMyBatery import MyBatery
import py_dss_interface
dss = py_dss_interface.DSS()

MyBat = MyBatery(dss=dss,
                 kw_rated=10,
                 kwh_rated=500,
                 soc=0.5,
                 eff_charge=0.91,
                 eff_discharge=0.91,
                 standby_loss=0.01)

dss.text("Clear")
dss.text("New Circuit.Source bus1=A basekv=0.48 phases=3 pu=1.0")
dss.text("New Linecode.LC1 nphases=3 R1=0.02794 X1=0.02502 units=km")
dss.text("New Line.Linha1 bus1=A bus2=B linecode=LC1 length=10 units=km")
dss.text("New Load.CargaFixa phases=3 bus1=B kv=0.48 kw=10 conn=wye model=1")
dss.text("Set voltagebases=[0.48]")
dss.text("Calcvoltagebases")
dss.text("Set mode=Snapshot")


# ADICIONAR BATERIA NA BARRA DA SUBESTAÇÃO
MyBat.AdicionarBateriaSubestacao(Nome='subsbatery')


horas = list(range(25)) 
p_carga_lista = []
p_rede_lista = []

# Parâmetros Nominais
kw_rated = 10.0
kwh_rated = 500.0
soc_atual = 50.0  

kwcargaFixa = 30

energia_atual = (soc_atual / 100.0) * kwh_rated 

soc_lista = [soc_atual] 
soc_2 = [soc_atual]
kwh_lista = [energia_atual]
kwh_listav2 = [energia_atual]
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
        if Nome.startswith("subsbatery"):
            
            if loadshapeCargafixa[h] > 0.3:
                p_simulada = kw_rated * loadshapeCargafixa[h] * 3
                MyBat.kw = -1*kw_rated*loadshapeCargafixa[h] * 3

            elif loadshapeCargafixa[h] <= 0:
                p_simulada = -1 * kw_rated 
                
                MyBat.kw = kw_rated*0.4
                
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
    
    soc_2.append(MyBat.soc_percent)
    kwh_lista.append(energia_atual)
    kwh_listav2.append(MyBat.kwh_store)

# Ajuste de sincronismo visual
p_carga_lista.append(p_carga_lista[-1])
p_rede_lista.append(p_rede_lista[-1])
v_min_lista.append(v_min_lista[-1])

# Plot
graphics = Graphics(p_carga_lista,
                    p_rede_lista,
                    v_min_lista,
                    horas,
                    soc_2,
                    kwh_lista,
                    kwh_listav2)

graphics.PlotSimulacao()




