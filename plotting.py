import pandas
from IPython.display import display
from matplotlib import pyplot as plt, axes
import numpy as np
import matplotlib.dates as mdates
import datetime
from pandas.plotting import scatter_matrix


def plot_parameter(wq, value: str, title: str):
    import matplotlib.dates as mdates

    fig, ax = plt.subplots(figsize=(12, 8))
    fig.subplots_adjust(right=0.75)

    twin1 = ax.twinx()
    twin2 = ax.twinx()

    twin2.spines.right.set_position(("axes", 1.2))

    if value == 'median':
        p1, p2, p3 = __plot_median(wq, ax, twin1, twin2)
    elif value == 'avg':
        p1, p2, p3 = __plot_avg(wq, ax, twin1, twin2)
    elif value == 'max':
        p1, p2, p3 = __plot_max(wq, ax, twin1, twin2)
    else:
        raise ValueError('Must pass in either "median" or "avg" or "max" for function parameter "value"')

    ax.set_xlabel("Interval (120 days)")
    ax.set_ylabel("Turbidity (NTU)")
    twin1.set_ylabel("Conductivity (uS/cm @25 C)")
    twin2.set_ylabel("E. Coli (cfu/100mL)")

    ax.yaxis.label.set_color(p1.get_color())
    twin1.yaxis.label.set_color(p2.get_color())
    twin2.yaxis.label.set_color(p3.get_color())

    tkw = dict(size=4, width=1.5)
    ax.tick_params(axis='y', colors=p1.get_color(), **tkw)
    twin1.tick_params(axis='y', colors=p2.get_color(), **tkw)
    twin2.tick_params(axis='y', colors=p3.get_color(), **tkw)
    ax.tick_params(axis='x', **tkw)

    start_date = datetime.date(2012, 7, 1)
    end_date = datetime.date(2013, 9, 1)

    ylims = ax.get_ylim()
    ax.vlines(start_date, ylims[0], ylims[1] * .66, label="Construction Started", linestyles='--')
    ax.text(start_date, ylims[1] * .66, f"Construction Started {start_date.strftime('%x')}", ha='right', va='top', rotation=90)

    ax.vlines(end_date, ylims[0], ylims[1] * .66, label="Construction Ended", linestyles='--')
    ax.text(end_date, ylims[1] * .66, f"Construction Ended {end_date.strftime('%x')}", ha='right', va='top', rotation=90)

    ax.set_ylim(ylims)

    fig.tight_layout()
    ax.xaxis.set_major_locator(mdates.YearLocator())
    fig.autofmt_xdate()

    ax.legend(handles=[p1, p2, p3])

    plt.title(title)

    plt.show()


def __plot_median(wq, ax: axes.Axes, twin1: axes.Axes, twin2: axes.Axes):
    p1, = ax.plot(wq.start_date, wq.median_value_turbidity, color='blue', marker='o',
                  label="Median Turbidity")
    p2, = twin1.plot(wq.start_date, wq.median_value_conductivity, color='red', marker='o',
                     label="Median Conductivity")
    p3, = twin2.plot(wq.start_date, wq.median_value_ecoli, color='green', marker='o',
                     label="Median E. Coli")
    return p1, p2, p3


def __plot_max(wq, ax: axes.Axes, twin1: axes.Axes, twin2: axes.Axes):
    p1, = ax.plot(wq.start_date, wq.max_value_turbidity, color='blue', marker='o',
                  label="Peak Turbidity")
    p2, = twin1.plot(wq.start_date, wq.max_value_conductivity, color='red', marker='o',
                     label="Peak Conductivity")
    p3, = twin2.plot(wq.start_date, wq.max_value_ecoli, color='green', marker='o',
                     label="Peak E. Coli")

    return p1, p2, p3


def __plot_avg(wq, ax: axes.Axes, twin1: axes.Axes, twin2: axes.Axes):
    p1, = ax.plot(wq.start_date, wq.avg_value_turbidity, color='blue', marker='o',
                  label="Avg Turbidity")
    p2, = twin1.plot(wq.start_date, wq.median_value_conductivity, color='red', marker='o',
                     label="Avg Conductivity")
    p3, = twin2.plot(wq.start_date, wq.median_value_ecoli, color='green', marker='o',
                     label="Avg E. Coli")
    return p1, p2, p3


def plot_correlation(joined: pandas.DataFrame, title: str):
    scatter_matrix(joined, figsize=(33, 33))
    plt.suptitle(
        f'Scatter Matrix of {title}',
        fontsize=20)
    plt.xticks(fontsize=16)
    plt.yticks(fontsize=16)
    plt.show()

    numerics = joined.drop(['start_date'], axis=1)

    fig = plt.figure(figsize=(33, 33))
    ax = fig.add_subplot(111)
    cax = ax.matshow(numerics.corr(method='spearman'), vmin=-1, vmax=1)
    fig.colorbar(cax)
    ticks = np.arange(0, len(numerics.columns), 1)
    ax.set_xticks(ticks)
    ax.set_yticks(ticks)
    ax.set_xticklabels(numerics.columns, rotation=90)
    ax.set_yticklabels(numerics.columns)
    plt.title(
        f'Correlation matrix of {title}',
        fontsize=20)
    plt.show()

    display(numerics.corr(method='spearman'))
