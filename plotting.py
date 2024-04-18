import pandas
from IPython.display import display
from matplotlib import pyplot as plt
import numpy as np
import matplotlib.dates as mdates
import datetime
from pandas.plotting import scatter_matrix


def plot_parameter(wq, filter_parameter: str, date_cutoff: str, units: dict[str, str], interval: int, location: str):
    fig, ax1 = plt.subplots(figsize=(33, 33))

    tkw = dict(size=4, width=0.5)

    color = 'tab:red'
    ax1.set_xlabel(f'intervals ({interval} days)', fontsize=18)
    ax1.set_ylabel(f'{filter_parameter} ({units[filter_parameter]})', color=color, fontsize=18)

    ax1.plot(wq["start_date"], wq["avg_value"], color=color, marker='o')
    ax1.tick_params(axis='y', labelcolor=color, **tkw)
    ax1.tick_params(axis='x', **tkw)

    fig.tight_layout()
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m/%d/%Y'))
    fig.autofmt_xdate()

    plt.title(f'Avg values of intervals {date_cutoff} of {location}: {filter_parameter} ({units[filter_parameter]})',
              fontsize=20)
    plt.show()


def plot_correlation(joined: pandas.DataFrame, title: str):
    display(joined.describe())

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
