import matplotlib.pyplot as plt


def plot_result(postgres_result, mongo_result, title):
    postgres_times, postgres_rows = zip(*postgres_result)
    mongo_times, mongo_rows = zip(*postgres_result)

    fig, ax = plt.subplots()

    bar_width = 0.35
    index = range(len(postgres_times))

    bar1 = ax.bar(index, postgres_times, bar_width, label='Postgres')
    bar2 = ax.bar([i + bar_width for i in index], mongo_times, bar_width, label='MongoDB')

    ax.set_xlabel('Number of Rows')
    ax.set_ylabel('Time (seconds)')
    ax.set_title(title)
    ax.set_xticks([i + bar_width / 2 for i in index])
    ax.set_xticklabels(postgres_rows)
    ax.legend()

    plt.savefig(f'result/{title}.png')
