from typing import Any

class MyBatery:
    def __init__(self, dss: Any,
                 kw_rated: float,
                 kwh_rated: float,
                 soc: float,
                 eff_charge: float,
                 eff_discharge: float,
                 standby_loss: float):
        
        self.kwh_rated = kwh_rated
        self.kw_rated = kw_rated
        self.eff_charge = eff_charge
        self.eff_discharge = eff_discharge
        self.standby_loss = standby_loss
        self.dss = dss
        self._soc = soc
        
        self._kw_atual = 0.0
        
        
    
    def AdicionarBateriaSubestacao(self, Nome: str) -> None:
        """Adiciona Bateria na barra da Subestação"""
        kv=self.dss.vsources.base_kv
        cmd = f"New Load.{Nome} phases=3 bus1=A kv={kv} kw=0 conn=wye model=1"
        self.dss.text(cmd)

    @property
    def kw(self):
        return self._kw_atual
    
    @property
    def Soc(self):
        return self._soc
    
    @property
    def soc_percent(self):
        return self._soc * 100
    
    @property
    def kwh_store(self):
        return self._soc * self.kwh_rated
    
    @kw.setter
    def kw(self, valor_alvo: float):
        """
        Sempre que fizer 'bateria.kw = X, este bloco roda.
        Valor > 0: carregando
        Valor < 0: Descarregando
        """
        self.dss.loads.kw = valor_alvo
        self._kw_atual = valor_alvo
        self.dss.text(f"Edit Load.subsbatery kw={self._kw_atual}")
        
        self.soc()
    
    def soc(self):
        """Calcula o quanto de energia restou baseado
        no kw atual"""
        if self._kw_atual > 0:
            delta = (self._kw_atual * (1 + (1 - self.eff_charge))) / self.kwh_rated
        else:
            delta = (self._kw_atual * (1 + (1 - self.eff_discharge))) / self.kwh_rated
        
        self._soc = max(0.0, min(1.0, self._soc + delta))
    
        
    # def Storekwh(self):
    #     """Energia Armazenada em kwh"""
        
    #     return    
    
    
    # def EffCharge(self):
    #     """Eficiência no carregamento"""
        
    #     return None
    
    # def EffDischarge(self):
    #     """Eficiência no Descarregamento"""
    
    #     return None
    
    # def Idlingkw(self):
    #     """Perdas em Vazio/Stanby"""
        
    #     return 
    
    # def Reserv(self):
    #     """Reserva de Energia"""
        
    #     return
    
    # def KvarMax(self):
    #     """Injeção de Reativos até KvarMax"""
        
    #     return
    
    
    # def WattPriority(self):
    #     """Injeção/Absorção de Ativo é priorida"""
        
    #     return
    
    
    # def Soc(self):
    #     """State of Charge"""
        
    #     return
    
    
    