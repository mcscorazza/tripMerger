import numpy as np
import matplotlib.pyplot as plt


def sig2ext(sig, plot=False, titulo='Teste'):

    # Versao 02: esta possui os plots indicando quais pontos foram mantidos ou não

    """

    Find local extrema in a time history (signal).

    Parameters:
    sig : ndarray
        Time history of the signal.
    dt : float or ndarray, optional
        Sampling time (scalar) or time vector. Defaults to 1.
    clsn : int, optional
        Number of classes for the signal. If None, no class division is performed.

    Returns:
    ext : ndarray
        Found extrema (turning points of min and max type) in the signal.
    exttime : ndarray
        Times of extrema occurrence.

    np.diff(sig): Calcula a diferença entre elementos consecutivos do sinal. Isso resulta em um array que indica se o sinal está subindo (+) ou descendo (-).
    w1[:-1] * w1[1:] <= 0: Este trecho identifica onde há mudança de sinal, ou seja, quando sig inverte sua tendência (de crescente para decrescente ou vice-versa), marcando os extremos locais.
    w: Constrói um array booleano onde True indica os extremos.
    ext = sig[w]: Extrai do sinal apenas os valores correspondentes a esses extremos.
    Resultado: ext agora contém apenas os valores dos extremos locais.

    np.diff(ext): Calcula a diferença entre valores consecutivos dos extremos.
    (w1[:-1] != 0) | (w1[1:] != 0): Garante que, se um valor for repetido consecutivamente (triplo, quádruplo etc.), pelo menos um deles seja removido.
    ext = ext[w]: Atualiza ext, removendo valores redundantes.
    Resultado: ext agora contém apenas os pontos de inversão necessários, eliminando repetições.

    ext[:-1] != ext[1:]: Verifica se há valores consecutivos idênticos e marca para remoção.
    w[-1] = True: Garante que o último elemento seja mantido.
    ext = ext[w]: Atualiza ext, eliminando pontos redundantes.
    Resultado: ext contém apenas valores únicos e representativos dos turning points.

    np.diff(ext): Calcula as diferenças entre extremos consecutivos.
    (w1[:-1] * w1[1:] < 0): Verifica se há inversões válidas (de máximo para mínimo ou vice-versa).
    w = w[:len(ext)]: Ajusta o tamanho do vetor booleano.
    ext = ext[w]: Mantém apenas os pontos extremos que realmente alternam entre máximos e mínimos.

    """

    sig = np.asarray(sig).flatten()

    if len(sig) < 2:
        return sig

    sig_original = sig
    sig_original_x = np.arange(len(sig_original))

    x = np.arange(len(sig))  # Criando um eixo de tempo fictício

    try:
        # Encontrar extremos
        w1 = np.diff(sig) # diff pega a diferença entre dois elementos consecutivos. Logo, dá pra saber se está "subindo" ou "descendo"
        w = np.concatenate(([1], (w1[:-1] * w1[1:] <= 0).astype(int), [1])).astype(bool) # w é um array booleano (tipo mascara), que indica em quais posições ocorrem os extremos
        ext = sig[w] # ext vai conter os sinais apenas marcados como true (logo, que sao os extremos). Este é o vetor de extremos
        ext_x = x[w] # Mantém os índices correspondentes aos valores extremos

        ext_1 = ext
        ext_x_1 = ext_x

        if plot:
            plt.figure(figsize=(10, 6))
            plt.plot(sig_original_x, sig_original, label='Sinal Original', alpha=0.5)

        # Remover valores triplos
        w1 = np.diff(ext) # quando os valores são iguais, a diferença entre eles é zero.
        w = np.concatenate(([1], (w1[:-1] != 0) | (w1[1:] != 0), [1])).astype(bool) # mascara booleana, mantem true apenas para valores que não sao triplicados. Um valor será mantido se a diferença antes ou depois dele for diferente de 0. Garante que o primeiro e o último valores sempre sejam mantidos os [1] no inicio e fim do concatenate
        ext = ext[w]
        ext_x = ext_x[w]

        ext_2 = ext
        ext_x_2 = ext_x

        if plot:
            plt.scatter(ext_x, ext, color='orange', label='Após Remover Triplos', marker='s')

        # Remover pares consecutivos idênticos
        w = np.zeros_like(ext, dtype=bool)
        w[:-1] = ext[:-1] != ext[1:]
        w[-1] = True
        ext = ext[w]
        ext_x = ext_x[w]

        ext_3 = ext
        ext_x_3 = ext_x

        if plot:
            plt.scatter(ext_x, ext, color='red', label='Após Remover Duplicados', marker='x')


        # Verificação final
        if len(ext) > 2:
            w1 = np.diff(ext)
            w = np.concatenate(([True], (w1[:-1] * w1[1:] < 0), [True])) # Aqui verifica se existem apenas turning points
            w = w[:len(ext)]
            ext = ext[w]
            ext_x = ext_x[w]

            ext_4 = ext
            ext_x_4 = ext_x

        if plot:
            # plt.scatter(ext_x, ext, color='green', label='Extremos Finais 111', marker='D', zorder=1)
            plt.scatter(sig_original_x, sig_original, color='purple', label='Dados iniciais', marker='+', zorder=1)
            plt.scatter(ext_x_1, ext_1, color='red', label='Passo 1', marker='4', zorder=2)
            plt.scatter(ext_x_2, ext_2, color='blue', label='Passo 2', marker='<', zorder=3)
            plt.scatter(ext_x_3, ext_3, color='yellow', label='Passo 3', marker='>', zorder=4)
            plt.scatter(ext_x_4, ext_4, color='violet', label='Passo 4 (fim)', marker='*', zorder=5)

            plt.legend(loc='upper right')
            plt.xlabel('Índice do Sinal')
            plt.ylabel('Amplitude')
            plt.title(titulo)
            plt.grid()
            plt.show()

        return ext

    except Exception as e:
        print(e)
        print("~~~~~~~~~~~~ falhou ao detectar extremos do sinal ~~~~~~~~~~~~")

        # Entao retorno um array com zero
        return np.asarray([0])
