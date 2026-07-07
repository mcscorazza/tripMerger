import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np

# ============================================================
# Dados da FAT-140
# ============================================================
S_D = 101.5   # MPa (Stress Range no joelho)
N_D = 1e7     # ciclos (no joelho)

m1 = 5        # slope antes do joelho (1e4 a 1e7)
m3 = 9        # slope depois do joelho (1e7 a 1e9) - valor "nominal"

N_min = 1e4
N_max = 1e9

# Ponto fornecido (região antes do joelho)
N_fat = 2e6
S_fat = 140.0

titulo = 'FAT-140'


# # ============================================================
# # Dados da FAT-90
# # ============================================================
# S_D = 52.7   # MPa (Stress Range no joelho)
# N_D = 1e7     # ciclos (joelho)
#
# m1 = 3        # slope antes do joelho (1e4 a 1e7)
# m3 = 5        # slope depois do joelho (1e7 a 1e9) - valor "nominal"
#
# N_min = 1e4
# N_max = 1e9
#
# # Ponto fornecido (região antes do joelho)
# N_fat = 2e6
# S_fat = 90
#
# titulo = 'FAT-90'

# ============================================================
# Funções auxiliares
# ============================================================
def m_from_knee_and_point(Nk, Sk, Np, Sp):
    """
    Calcula o slope m (no formato S^m * N = const) que conecta:
      (Nk, Sk) -> (Np, Sp)
    usando: Sp/Sk = (Nk/Np)^(1/m)
    => m = log(Nk/Np) / log(Sp/Sk)
    """

    print(f"Nk: {Nk} | Sk: {Sk} | Np: {Np} | Sp: {Sp}")

    return np.log10(Nk / Np) / np.log10(Sp / Sk)


def S_of_N(N, m3_used=9):
    """
    Retorna S (MPa) dado N (ciclos) para curva bi-linear com joelho em N_D.
    Usa m1 à esquerda e m3_used à direita.
    """
    N = np.asarray(N, dtype=float)
    S = np.empty_like(N) # Criando array vazio usando o N como base

    left = N <= N_D # Máscara booleana para definir que se N <= N_D vai usar slope m1
    right = ~left   # inverte a máscara "Left", então no restante, usa slope m3

    # Aplica a Equação de Basquin para cada trecho, já que tem slopes diferentes
    S[left] = S_D * (N_D / N[left]) ** (1.0 / m1)
    S[right] = S_D * (N_D / N[right]) ** (1.0 / m3_used)

    print()

    return S


def N_of_S(S, m3_used=9):
    """
    Retorna N (ciclos) dado S (MPa), selecionando automaticamente o trecho:
      - S >= S_D => usa m1 (lado esquerdo, N <= N_D)
      - S <  S_D => usa m3_used (lado direito, N >= N_D)
    Inversão de: S = S_D*(N_D/N)^(1/m)  =>  N = N_D*(S_D/S)^m
    """
    S = np.asarray(S, dtype=float)
    if np.any(S <= 0):
        raise ValueError("S deve ser > 0 (curva log-log).")

    N = np.empty_like(S)

    left = S >= S_D
    right = ~left

    N[left] = N_D * (S_D / S[left]) ** m1
    N[right] = N_D * (S_D / S[right]) ** m3_used

    return N


# ============================================================
# Plot e validações
# ============================================================
def plot_sn_curve(S_query=None, titulo='FAT X'):
    """
    Plota a curva S–N em log-log e:
    - marca o joelho, o ponto FAT e o ponto de inspeção
    - opcionalmente recalibra m3 para passar pelo ponto de inspeção (mantendo o joelho)

    S_query: se passado, calcula N automaticamente e marca no gráfico.

    """
    m3_used = m3

    # Curva
    N = np.logspace(np.log10(N_min), np.log10(N_max), num=100000) # Cria um vetor de Num pontos na escala log. Indo de 10^4 até 10^9. Quanto maior o valor de Num, maior é a precisão da curva. Por exemplo, com num = 10k o ponto de 10^7 na verdade ficou entre 9.995.395,42 e 10.006.910,83
    S = S_of_N(N, m3_used)

    # Checagens numéricas
    S_at_fat = float(S_of_N(np.array([N_fat]), m3_used)[0])

    # Plot
    plt.figure(figsize=(9.5, 5.8))
    plt.loglog(N, S, linewidth=2, label=f"{titulo} (m1={m1}, m3={m3_used:.3g})")

    # Joelho
    plt.scatter([N_D], [S_D], color="black", zorder=5)
    plt.annotate(f"Joelho\nN={N_D:.1e}\nS={S_D:.1f} MPa",
                 (N_D, S_D), textcoords="offset points", xytext=(10, 10))

    # Ponto FAT fornecido
    plt.scatter([N_fat], [S_fat], color="green", zorder=5)
    plt.annotate(f"FAT\nN={N_fat:.1e}\nS={S_fat:.0f} MPa",
                 (N_fat, S_fat), textcoords="offset points", xytext=(-30, -40))

    # Consulta S -> N (automático)
    if S_query is not None:
        N_found = float(N_of_S(np.array([S_query]), m3_used)[0])
        plt.scatter([N_found], [S_query], color="purple", zorder=7)
        plt.annotate(f"Consulta\nS={S_query:.2f} MPa\nN={N_found:.3e}",
                     (N_found, S_query), textcoords="offset points", xytext=(10, 10))
        print(f">>> Consulta: para S = {S_query:.3f} MPa  =>  N = {N_found:.6g} ciclos")

    plt.grid(True, which="both", linestyle="--", alpha=0.65)
    plt.xlabel("N (cycles)")
    plt.ylabel("Stress Range (MPa)")
    plt.title(f"Curva {titulo}")
    plt.xlim(1e4, 1e9)
    plt.legend()


    ax = plt.gca()

    # 1) MAJOR ticks nas décadas (100, 1000, ...)
    ax.yaxis.set_major_locator(mticker.LogLocator(base=10.0, subs=(1.0,), numticks=100))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda y, _: f"{y:g}"))  # sem notação científica

    # 2) MINOR ticks densos dentro de cada década: 1.0, 1.1, 1.2, ..., 9.9  (=> 210 aparece)
    subs_dense = np.round(np.arange(1.0, 10.0, 0.1), 1)  # 1.0..9.9 passo 0.1
    ax.yaxis.set_minor_locator(mticker.LogLocator(base=10.0, subs=subs_dense, numticks=1000))

    def minor_fmt(y, pos):
        """
        Mostra rótulos nos minor ticks dependendo do zoom.
        - Se muitas décadas visíveis: não rotula minor ticks.
        - Se zoom médio: rotula apenas passos de 0.5 na mantissa (ex.: 2.0, 2.5, 3.0 ...)
        - Se zoom forte: rotula passos de 0.1 (ex.: 2.1 => 210)
        """
        if y <= 0:
            return ""

        y0, y1 = ax.get_ylim()
        decades_visible = abs(np.log10(y1) - np.log10(y0))

        exp = np.floor(np.log10(y))
        mantissa = y / (10 ** exp)  # entre 1 e 10
        mantissa = np.round(mantissa, 1)  # evita ruído numérico

        # Níveis de detalhamento conforme zoom
        if decades_visible > 1.2:
            # Longe: sem rótulos nos minor ticks
            return ""

        if 0.8 < decades_visible <= 1.2:
            # Zoom médio: só mantissas múltiplas de 0.5 (2.0, 2.5, 3.0, ...)
            if np.isclose((mantissa * 10) % 5, 0, atol=1e-9):
                return f"{y:.2f}" if y >= 100 else f"{y:g}"
            return ""

        # Zoom forte: rotula mantissas múltiplas de 0.1 (2.1, 2.2, ..., 2.9 etc.)
        if np.isclose((mantissa * 10) % 1, 0, atol=1e-9):
            return f"{y:.2f}" if y >= 100 else f"{y:g}"

        return ""

    ax.yaxis.set_minor_formatter(mticker.FuncFormatter(minor_fmt))

    plt.show()


# ============================================================
# Exemplo do plot
# ============================================================
if __name__ == "__main__":
    plot_sn_curve(titulo=titulo)


