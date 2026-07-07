import numpy as np
from rainflow.funcs import (
    filtrar_sinal_passabaixa,
    agrupar_dados_Rainflow
)
from rainflow.Rainflow03 import (
  processo_Rainflow,
  tratar_dados_tensao_booleano,
  calcular_dano_FAT
  )
from rainflow.Sig2ext import sig2ext

class CalculadoraDanoFadiga:
    def __init__(self, modulo_elasticidade=0.198, freq_corte_lpf=15, delta_hysteresis=2, limite_fat=60.85):
        self.E = modulo_elasticidade
        self.freq_corte = freq_corte_lpf
        self.delta = delta_hysteresis
        self.limite_fat = limite_fat

    def calcular_dano(self, array_dados):
        dados = np.array(array_dados)
        dados = filtrar_sinal_passabaixa(dados, self.freq_corte)
        dados = dados * self.E
        tp_1 = sig2ext(dados, plot=False)
        dados = np.array(tp_1.tolist())
        dados = np.floor(dados + 0.5).astype(int)

        dados = tratar_dados_tensao_booleano(
            dados, 
            titulo="", 
            delta=self.delta, 
            plot=False
        )

        tp_2 = sig2ext(dados, plot=False)
        dados_finais = tp_2.tolist()
        rainflow_dados = processo_Rainflow(dados_finais)
        rainflow_agrupado = agrupar_dados_Rainflow(rainflow_dados)
        dano = calcular_dano_FAT(rainflow_agrupado, limite=self.limite_fat)
        return dano