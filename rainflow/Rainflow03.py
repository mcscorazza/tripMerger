# -------------------------------------------------------------------------------------------------------------------- #
# Rainflow03: com filtro de hysteresis (setar variavel 'delta').
#
# Faz tudo igual ao Rainflow02:
#
# # - Converte os dados originais do engate somente nos turning points
# # - Depois, contabiliza o ciclo com o Rainflow baseado na ASTM E1049-85
# # - Por fim, calcula o dano no batente dianteiro e traseiro
# # - Cria um arquivo Excel que exibe a resposta individual para cada viagem. Ira tambem somar os danos para mostrar o acumulado do "processamento".
#
# Porém, este aqui possui um filtro de hysteresis. Este aqui
# também plota os graficos para indicar quais os pontos que foram removidos/mantidos
#
# Utiliza diretamente o arquivo .TEM
# pip install -r requirements.txt
# -------------------------------------------------------------------------------------------------------------------- #

import numpy as np
import matplotlib.pyplot as plt
from CurvaSN import N_of_S, S_of_N


def rainflow(series):
    """
    Implements the Rainflow Counting Algorithm according to ASTM E1049-85.
    Parameters:
    - series (list or ndarray): Input time series of stress or strain values.
    Returns:
    - cycles (list of tuples): List of cycles with (amplitude, mean, count).
    """
    series = np.asarray(series)
    stack = [] # Stack for peaks and valleys
    cycles = [] # List of identified cycles
    start_point = None # Starting point of the series
    tamanhoSeries = len(series)

    print("Entrou na funcao rainflow()")

    def count_cycle(max, min, range_val, mean_val, full_cycle=True):

        # Record a full or half cycle with the given range and mean.
        # Parameters:
        # - range_val: range of the cycle.
        # - mean_val: Mean value of the cycle.
        # - full_cycle: If True, counts as a full cycle; else, as a half cycle.
        count = 1.0 if full_cycle else 0.5
        cycles.append((max, min, range_val, mean_val, count))
        # print(f"add ciclo: range {range_val} e count {count}")
    # Fim da funcao count_cycle


    contouCicloInteiro = False
    for i, point in enumerate(series):
        stack.append(point)
        # print(f"Stack: {stack}")
        if start_point is None:
            start_point = point # Initialize the starting point
        while len(stack) >= 3:
            # Form ranges X and Y using the last three points in the stack
            x = stack[-3:]

            range_y = abs(x[1] - x[0]) # Range Y = AB
            range_x = abs(x[2] - x[1]) # Range X = BC

            # print(f"range_y: {range_y}")
            # print(f"range_x: {range_x}")


            if range_x >= range_y:
                # Full cycle detected for range Y
                mean_y = (x[0] + x[1]) / 2
                SMax = x[0]
                SMin = x[1]
                # print(f"x[0]: {x[0]}")
                # print(f"x[1]: {x[1]}")
                # print(f"range_y: {range_y}")
                # print(f"mean_y: {mean_y}")
                # print()
                if x[0] == start_point or x[1] == start_point:
                    # Y contains the starting point, count as half cycle
                    count_cycle(SMax, SMin, range_y, mean_y, full_cycle=False)
                    start_point = x[1] # Move starting point
                    stack.pop(-3) # Remove o primeiro ponto do Y
                else:
                    # Y is a full cycle
                    count_cycle(SMax, SMin, range_y, mean_y, full_cycle=True)
                    stack.pop(-3) # Removendo último ponto do Y
                    stack.pop(-2) # Removendo o primeiro ponto de Y
                    # contouCicloInteiro = True
                    # break # Para nao executar o pop abaixo. Isso é o mesmo que GOTO Step 2
            else:
                break


        if i % 100000 == 0:
            # print(i)
            print(f'Processou: {i}/{tamanhoSeries}')

    # Process remaining ranges as half cycles
    while len(stack) > 1:
        range_val = abs(stack[-1] - stack[-2])
        mean_val = (stack[-1] + stack[-2]) / 2
        SMax = stack[-1]
        SMin = stack[-2]
        count_cycle(SMax, SMin, range_val, mean_val, full_cycle=False)
        stack.pop()

    return cycles



def visualize_cycles(series, cycles):

    # Visualize the time series data and overlay the identified rainflow cycles.
    # Parameters:
    # - series (list or ndarray): Input time series of stress or strain values.
    # - cycles (list of tuples): List of cycles with (range, mean, count).

    plt.figure(figsize=(10, 6))
    plt.plot(series, label="Time Series", color="blue", marker="o")

    # Overlay cycles
    for i, (range, mean, count) in enumerate(cycles):
        plt.axhline(mean + range / 2, color="red", linestyle="--", linewidth=0.8, label="Cycle Upper" if i == 0 else None)
        plt.axhline(mean - range / 2, color="green", linestyle="--", linewidth=0.8, label="Cycle Lower" if i == 0 else None)

    plt.title("Rainflow Cycles Visualization")
    plt.xlabel("Index")
    plt.ylabel("Value")
    plt.legend(loc='upper right')
    plt.grid()
    plt.show()





def carregar_dados(sFilename, canal):

    print("Carregando dados...")

    arqTEM = win32com.client.Dispatch("LynxFile.FileTS")

    data = []  # Cria uma lista vazia para acumular os dados

    if (arqTEM.openFile(sFilename)):
        # print(f"Sucesso na leitura do arquivo: {sFilename}.")

        # Defino algumas variaveis globais, pois o codigo inteiro vai ser referenciado nestas variaveis
        global sFilenameTEM, dtDataHoraInicio, sComentario, iNumCanais, iNumSamples, fFreqAquisicao

        sFilenameTEM = arqTEM.FileName
        dtDataHoraInicio = arqTEM.AcqDatetime
        sComentario = arqTEM.Comment
        iNumCanais = arqTEM.nChannels
        iNumSamples = arqTEM.nSamples
        fFreqAquisicao = arqTEM.SampleFreq
        # print()
        # print(f"iNumCanais: {iNumCanais}" )
        # print(arqTEM.AcqDateTime)

        listaR = []
        listaBuf = []
        listaNOut = []
        listaNumCanais = []
        listaNomeCanais = []
        listaUnidadesEngenharia = []

        arrayDados = np.zeros(iNumSamples)
        r, Buf, NOut = arqTEM.ReadBuffer(canal, 0, len(arrayDados), arrayDados)
        # r é um bool. Buf é uma tupla. NOut é um int (r, Buf, NOut) = obj.ReadBuffer...

        # r, Pcil, NOut = arqTEM.ReadBuffer(canal, 0, len(arrayDados), arrayDados)

        ### Trecho de codigo abaixo: anda por todos os canais
        # for i in range(0, iNumCanais):
        #     # O canal aqui começa em 0
        #
        #     iNumCanal = arqTEM.MapSnToCh(i)
        #     sNomeCanal = arqTEM.SnName(i)
        #     sUnidadeEngenharia = arqTEM.SnUnit(i)
        #     dtDataHoraInicio = arqTEM.AcqDatetime
        #     iNumSamples = arqTEM.nSamples
        #     fFreqAquisicao = arqTEM.SampleFreq
        #     print(f"Canal numero {arqTEM.MapSnToCh(i)} (total: {iNumCanais-1})]: {arqTEM.SnName(i)} - unidade de engenharia: {arqTEM.SnUnit(i)}")
        #
        #     arrayDados = np.zeros(iNumSamples)
        #     # r, Buf, NOut = arqTEM.ReadBuffer(i, 0, len(arrayDados), arrayDados) #TODO Comentado para testes
        #     # r é um bool. Buf é uma tupla. NOut é um int
        #
        #     r = 0
        #     Buf = 0
        #     NOut = 0
        #
        #     listaR.append(r)
        #     listaBuf.append(Buf)
        #     listaNOut.append(NOut)
        #     listaNumCanais.append(iNumCanal)
        #     listaNomeCanais.append(sNomeCanal)
        #     listaUnidadesEngenharia.append(sUnidadeEngenharia)

        return np.asarray(Buf), sFilenameTEM




def converter_tensao_forca(dados):

    ############################################################################################
    # Faço a conversão de kgf para tensão, segundo a tabela:
    # TODO Ter ciencia de que estes parametros sao do HTT 1.0 (disponibilizados pelo Matheus)
    ############################################################################################
    # Batente Dianteiro:
    # Tracao: 264 tf >>>> 425 MPa
    # Compressao: -169 tf >>>> 36 MPa

    # Batente Traseiro:
    # Tracao: 264 tf >>>> 261 MPa
    # Compressao: -169 tf >>>> 68 MPa

    # Raciocinio, para batente dianteiro:
    # - Se o valor for positivo, calcula-se a reta de 0 até 264000 kgf (264 tf) que vai de 0 até 425 MPa
    # Tensao = 0.001609848484848 * Forca
    # - Se o valor for negativo, calcula-se uma reta de 0 até -169000 kgf (-169 tf) que vai de 0 até 36 MPa
    # Tensao = 2,1301775147928e-4 * Forca

    # Raciocinio, para batente traseiro:
    # - Se o valor for positivo, calcula-se a reta de 0 até 264000 kgf (264 tf) que vai de 0 até 261 MPa
    # Tensao = 9,886363636363636e-4 * Forca
    # - Se o valor for negativo, calcula-se uma reta de 0 até -169000 kgf (-169 tf) que vai de 0 até 68 MPa
    # Tensao = 4,023668639053254e-4 * Forca

    dados = np.asarray(dados)

    fFatorBatenteDianteiroTracao = 0.001609848484848 # Tracao, valores em kgf positivos
    fFatorBatenteDianteiroCompressao = 2.1301775147928e-4 # Compressao, valores em kgf negativos

    fFatorBatenteTraseiroTracao = 9.886363636363636e-4
    fFatorBatenteTraseiroCompressao = 4.023668639053254e-4

    dados_convertidos_batente_dianteiro = np.where(dados > 0, (dados * fFatorBatenteDianteiroTracao), (dados * fFatorBatenteDianteiroCompressao))
    dados_convertidos_batente_traseiro = np.where(dados > 0, (dados * fFatorBatenteTraseiroTracao), (dados * fFatorBatenteTraseiroCompressao))


    return dados_convertidos_batente_dianteiro, dados_convertidos_batente_traseiro


def plotar(dado1, dado2, titulo):

    xIni = list(range(len(dado1)))
    xFim = list(range(len(dado2)))

    # # Filtra o gráfico pra não ficar tão pesado
    # N = 10000000
    #
    # xFim = xFim[:N]
    # xIni = xIni[:N]
    # dado1 = dado1[:N]
    # dado2 = dado2[:N]

    plt.plot(xIni, dado1, marker='x', label='Antes', color='r')
    plt.plot(xFim, dado2, marker="o", label='Depois', color='b')
    plt.title(titulo)
    plt.xlabel('Amostras')
    plt.ylabel('Unidade Y')
    plt.legend(loc='upper right')
    plt.show()


def plotar_subplot(dado1, dado2):

    # # Filtra o gráfico pra não ficar tão pesado
    # N = 10000000
    #
    # xFim = xFim[:N]
    # xIni = xIni[:N]
    # dado1 = dado1[:N]
    # dado2 = dado2[:N]

    xIni = list(range(len(dado1)))
    xFim = list(range(len(dado2)))

    # Criando a figura e os subplots (1 linha, 2 colunas)
    fig, axs = plt.subplots(2, 1, sharex=True, figsize=(10, 4))

    # Primeiro subplot
    axs[0].plot(xIni, dado1, marker='p', label="Dado 1 - ini", color="blue")
    axs[0].set_title("Sinal original")
    axs[0].legend(loc='upper right')
    axs[0].set_xlabel("Amostras")
    axs[0].set_ylabel("Tensão (MPa)")
    axs[0].grid()

    # Segundo subplot
    axs[1].plot(xFim, dado2, marker='o', label="Dado 2 - fim", color="red")
    axs[1].set_title("Sinal alterado")
    axs[1].legend(loc='upper right')
    axs[1].set_xlabel("Amostras")
    axs[1].set_ylabel("Tensão (MPa)")
    axs[1].grid()

    # Ajustando espaçamento entre os gráficos
    plt.tight_layout()

    # Exibir o gráfico
    plt.show()



########################################################################################################################
# Funcao: processo_Rainflow
# Input: dados - tipo numpy array ou similar
# Output: Range/Mean/Count do Rainflow, com agrupamento de ciclos de mesmo valor e com arredondamento de casa decimais
#
# Obs: Esta funcao do process_Rainflow(dados) está adaptada para gerar os ciclos de Rainflow com N casas decimais
# e agrupando ciclos que possuem o mesmo range/mean. Sempre verificar se ela esta de acordo com o objetivo desejável.
########################################################################################################################

def processo_Rainflow(dados):

    cycles = rainflow(dados)

    # for SMax, SMin, range, mean, count in cycles:
    #     print(f"SMax: {SMax}, SMin: {SMin}, Amplitude: {range}, Mean: {mean}, Count: {count}")

    # Criando uma lista só com a parte dos valores
    SMax = np.array([a[0] for a in cycles])
    SMin = np.array([b[1] for b in cycles])
    range = np.array([c[1] for c in cycles]) # Amplitude
    mean = np.array([d[1] for d in cycles])
    count = np.array([e[1] for e in cycles])

    # Criando uma lista com os valores arredondados. Lembrete: SMax e SMin já estão arredondados
    range_arredondado = np.round(range, 1)
    mean_arredondado = np.round(mean, 1)

    # Criando a nova lista com os valores arredondados
    cyclesTratado = [(a, b, c) for a, b, c in zip(SMax, SMin, count)] # Só essas 3 variáveis importam. O resto não.

    # for range, mean, count in cyclesTratado:
    #     print(f"Amplitude: {range}, Mean: {mean}, Count: {count}")

    # Agrupando os ciclos de mesma amplitude:
    # Dicionário para armazenar a soma dos ciclos por amplitude
    agrupados = {}
    agrupadosComMean = {}
    agrupadosTratados = {}
    agrupadosComMeanTratados = {}

    # Iterar sobre a lista
    for SMax, SMin, range, mean, count in cycles:
        chave = range

        if chave in agrupados:
            agrupados[chave] += count  # Soma os ciclos se a range já existe
            # print(f"chave repetido: {chave}")
        else:
            agrupados[chave] = count  # Adiciona a range ao dicionário

    # Converter de volta para uma lista de tuplas, se necessário
    resultado = [(range, count) for range, count in agrupados.items()]


    # for range_val, mean_val, count_val in cycles:
    #     chave = (range_val, mean_val)
    #     if chave in agrupadosComMean:
    #         agrupadosComMean[chave] += count_val
    #     else:
    #         agrupadosComMean[chave] = count_val
    #
    # # Converter de volta para lista de tuplas
    # resultadoComMean = [(range_val, mean_val, count_total) for (range_val, mean_val), count_total in agrupadosComMean.items()]

    # Ordeno o resultado de forma decrescente, considerando o valor do Range:
    resultado.sort(key=lambda x: x[0], reverse=True)

    print('=== encerrou print ===')

    return resultado






########################################################################################################################
# Funcao: tratar_dados_tensao_booleano
#
# Inputs:
# - dados: array da série de dados
# - titulo: titulo que é utilizado no grafico
# - delta: valor do gate, que só irá manter as amostras consecutivas quando a diferença entre elas superar o gate. O gate pode ser inteiro ou float.
#
# Output:
# - dadosAposFiltro: é o array resultante após aplicar o filtro de hysteresis
#
########################################################################################################################
def tratar_dados_tensao_booleano(dados, titulo, delta, plot=False):

    dados = np.asarray(dados)

    # Criar array booleano com o mesmo tamanho do vetor original
    mask = np.zeros(dados.shape, dtype=bool)
    mask[0] = True  # O primeiro elemento sempre é mantido, marco ele como true

    # Criar o array, guardando a primeira posicao
    dadosAposFiltro = [dados[0]]

    # Percorrer o array (a partir da segunda posicao) e marcar os valores mantidos de acordo com o delta definido
    for i in range(1, len(dados)):
        if abs(dados[i] - dadosAposFiltro[-1]) >= delta:
            dadosAposFiltro.append(dados[i])
            mask[i] = True  # Marca os valores mantidos

    # Converter dadosBatenteAposFiltro para array numpy
    dadosAposFiltro = np.array(dadosAposFiltro)

    # Criar os arrays de pontos mantidos e eliminados
    kept_points = dados[mask]
    removed_points = dados[~mask]

    # Criar índices para plot
    indices = np.arange(len(dados))

    # Plotar os pontos mantidos e removidos
    if (plot):
        plt.figure(figsize=(8, 5))
        plt.plot(indices, dados, 'o-', label="Original", alpha=0.5)  # Linha completa
        plt.scatter(indices[mask], kept_points, color='green', label="Mantidos", zorder=3)
        plt.scatter(indices[~mask], removed_points, color='red', label="Eliminados", zorder=3)
        plt.xlabel("Índice no array")
        plt.ylabel("Tensão (MPa)")
        plt.legend(loc='upper right')
        plt.title(titulo)
        plt.show()

    return dadosAposFiltro



def plotar_1var(dado1, titulo):

    # x = list(range(len(dado1)))

    print(titulo)
    print(f"len plotar_1var: {len(dado1)}")

    # # Filtra o gráfico pra não ficar tão pesado
    # N = 10000000
    #
    # xFim = xFim[:N]
    # xIni = xIni[:N]
    # dado1 = dado1[:N]
    # dado2 = dado2[:N]

    plt.plot(dado1, color='b', linewidth='0.5')
    plt.title(titulo)
    plt.xlabel('Amostras')
    plt.ylabel('Tensão (MPa)')
    # plt.legend(loc='upper right')
    plt.show()


def plotar_2var(dado1, dado2, titulo):

    # x = list(range(len(dado1)))

    print(titulo)
    print(f"len plotar_2var - dado1: {len(dado1)}")
    print(f"len plotar_2var - dado2: {len(dado2)}")

    # # Filtra o gráfico pra não ficar tão pesado
    # N = 10000000
    #
    # xFim = xFim[:N]
    # xIni = xIni[:N]
    # dado1 = dado1[:N]
    # dado2 = dado2[:N]

    plt.plot(dado1, color='b', linewidth='0.5')
    plt.plot(dado2, color='orange', linewidth='0.5')
    plt.title(titulo)
    plt.xlabel('Amostras')
    plt.ylabel('Tensão (MPa)')
    # plt.legend(loc='upper right')
    plt.show()


def calcular_dano_FAT(dadosRainflow, limite):

    # Explicacao sobre como encontrar o valor do limite:
    # Precisa executar o código no arquivo "CurvaSN.py" e analisar qual o valor de corte para 10^9 ciclos
    # Para a FAT-140, abaixo de ~60,85 já nao existe dano.
    # Para a FAT-90, abaixo de ~21 já não existe mais dano.

    print("Entrou na funcao calcular_dano_FAT")

    # Calcula o dano usando a regra de Miner
    fDanoAcumulado = 0
    cont = 1

    for i in dadosRainflow:

        iStressRange = i[0]
        iNumCiclos = i[1]  # Numero de ciclos aplicados (count)


        if iStressRange < limite:
            continue  # Pois nao contabiliza nada de dano. Vai para o próximo elemento.

        N = N_of_S(S=iStressRange) # Numero de ciclos para um valor de stress específico

        fDano = (iNumCiclos / N)
        fDanoAcumulado = fDanoAcumulado + fDano
        print(f"[{cont}/{len(dadosRainflow)}] | Stress range {iStressRange}, count {iNumCiclos} | N: {N} | Dano: {fDano} | Acumulado: {fDanoAcumulado}")
        cont = cont + 1

    # Fim do calculo de dano
    print(f"Finalizando.............. Dano Acumulado: {fDanoAcumulado}")

    return fDanoAcumulado

