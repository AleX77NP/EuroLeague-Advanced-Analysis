import matplotlib.pyplot as plt


class EuroLeaguePlotter:

    # Plot basic graph with X and Y values from DataFrame
    @staticmethod
    def plot_basic_graph(df, col_x, col_y, label_x, label_y, title, color="blue"):
        plt.figure(figsize=(8, 5))
        plt.bar(df[col_x], df[col_y], color=color)
        plt.xlabel(label_x)
        plt.ylabel(label_y)
        plt.title(title)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()
