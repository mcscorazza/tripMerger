from scipy import signal
import win32com.client
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

# def definir_variaveis_globais(sFilename):
#
#     print("Definindo variaveis globais...")
#
#     # global fFreqAquisicao, iNumCanais, iNumSamples, dtDataHoraInicio
#
#     fFreqAquisicao = float(arqMAT.get('FreqAquisicaoHz'))
#     iNumCanais = int((arqMAT.get('QtdCanais')))
#     iNumSamples = int((arqMAT.get('NumAmostras')))
#     dtDataHoraInicio = str(arqMAT.get('DataHoraInicio')) # 2024-12-14 19:41:00
#     dtDataHoraInicio = dtDataHoraInicio.replace("[", "").replace("]", "").replace("'", "")
#     dtDataHoraInicio = datetime.strptime(dtDataHoraInicio, "%Y-%m-%d %H:%M:%S")
#
#     print("Definiu.")


def printar_informacoes_gerais(sFilename, sNomeCanalDesejado):
    arqTEM = win32com.client.Dispatch("LynxFile.FileTS")

    print("=========================================================")

    if (arqTEM.openFile(sFilename)):
        print(f"Sucesso na leitura do arquivo!")
        print(f"Path: {arqTEM.FileName}")
        print(f"dtDataHoraInicio: {arqTEM.AcqDatetime}")
        print(f"sComentario: {arqTEM.Comment}")
        print(f"iNumCanais: {arqTEM.nChannels}")
        print(f"iNumSamples: {arqTEM.nSamples}")
        print(f"fFreqAquisicao: {arqTEM.SampleFreq}")

        global global_fFreqAquisicao
        global_fFreqAquisicao = int(arqTEM.SampleFreq)

        global global_iNumSamples
        global_iNumSamples = int(arqTEM.nSamples)

        global global_iNumCanais
        global_iNumCanais = int(arqTEM.nChannels)

        for i in range(0, arqTEM.nChannels):
            # O canal aqui começa em 0

            iNumCanal = arqTEM.MapSnToCh(i)
            sNomeCanal = arqTEM.SnName(i)
            sUnidadeEngenharia = arqTEM.SnUnit(i)
            dtDataHoraInicio = arqTEM.AcqDatetime
            iNumSamples = int(arqTEM.nSamples)
            # print(f"Canal numero {arqTEM.MapSnToCh(i)} (total: {arqTEM.nChannels-1})]: {arqTEM.SnName(i)} - unidade de engenharia: {arqTEM.SnUnit(i)}")

            if sNomeCanalDesejado.lower() in sNomeCanal.lower():
                print(
                    f"Canal numero {arqTEM.MapSnToCh(i)} (total: {arqTEM.nChannels - 1})]: {arqTEM.SnName(i)} - unidade de engenharia: {arqTEM.SnUnit(i)}")
                print(f"Encontrou canal que contém '{sNomeCanalDesejado}' no nome. Canal: {i}")
                iCanalDesejado = i
                break # Quebro pois ja achei o canal que eu queria.
            else:
                iCanalDesejado = 888 # Retorno um valor absurdo foi não encontrou o canal com nome especificado
    else:
        print('Falha na leitura do arquivo.')
        iCanalDesejado = 999 # Retorno um valor absurdo

    print("=========================================================")

    return iCanalDesejado




def filtrar_sinal_passabaixa(sinal, fc):

    print("Low-pass filter...")

    fs = 200  # Cravando para testes na AWS

    # fs = global_fFreqAquisicao
    # T = global_iNumSamples / fs  # Duracao do sinal em segundos # Aqui era antes T = iNumAmostras / fs
    # t = np.arange(0, T, 1 / fs)  # Vetor de tempo

    numtaps = 500  # Número de coeficientes do filtro (tamanho do filtro)
    filtro = signal.firwin(numtaps, fc, fs=fs, pass_zero=True)

    # Filtfilt para já corrigir a defasagem linear
    sinal_filtrado_filtfilt = signal.filtfilt(filtro, 1.0, sinal)

    return sinal_filtrado_filtfilt





def agrupar_dados_Rainflow(RainFlowDados):

    resultado = defaultdict(int)
    print()
    for range_, count in RainFlowDados:
        resultado[range_] += count

    resultado = list(resultado.items())

    return resultado