from scipy import signal
from typing import Callable, Iterable, List, Tuple, Union
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP
import matplotlib.pyplot as plt

fFreqAquisicao = 0
global_fFreqAquisicao = 0

iNumCanais = 0
global_iNumCanais = 0

iNumSamples = 0
global_iNumSamples = 0

dtDataHoraInicio = 0
global_dtDataHoraInicio = 0

def filtrar_sinal_passabaixa(sinal, fc):
    print("Low-pass filter...")
    fs = 200  # Cravando para testes na AWS
    numtaps = 500  # Número de coeficientes do filtro (tamanho do filtro)
    filtro = signal.firwin(numtaps, fc, fs=fs, pass_zero=True)
    sinal_filtrado_filtfilt = signal.filtfilt(filtro, 1.0, sinal)
    print("✅ End LPF")
    return sinal_filtrado_filtfilt

def agrupar_dados_Rainflow(RainFlowDados):

    resultado = defaultdict(int)
    print()
    for range_, count in RainFlowDados:
        resultado[range_] += count

    resultado = list(resultado.items())

    return resultado