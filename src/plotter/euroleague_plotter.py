import matplotlib.pyplot as plt


class EuroLeaguePlotter:

    # Plot basic graph with X and Y values from DataFrame
    @staticmethod
    def plot_stats_graph(df, col_x, col_y, label_x, label_y, title, color="orange"):
        plt.figure(figsize=(8, 5))
        plt.bar(df[col_x], df[col_y], color=color)
        plt.xlabel(label_x)
        plt.ylabel(label_y)
        plt.title(title)
        plt.xticks(rotation=45, ha="right")
        plt.tight_layout()
        plt.show()

    @staticmethod
    def plot_model_feature_graph(feature_names, importance):
        colors = ["green" if importance > 0 else "red" for importance in importance]
        # Plot the feature importance
        plt.barh(range(len(feature_names)), importance, align="center", color=colors)
        plt.yticks(range(len(feature_names)), feature_names)
        plt.xlabel("Feature Importance")
        plt.title("Logistic Regression Feature Importance")
        plt.show()

    @staticmethod
    def plot_roc_curve(fpr, tpr, auc_roc):
        # Plot the ROC curve
        plt.figure(figsize=(8, 6))
        plt.plot(fpr, tpr, color="darkorange", lw=2, label=f"AUC = {auc_roc:.2f}")
        plt.plot([0, 1], [0, 1], color="navy", lw=2, linestyle="--")
        plt.xlim([0.0, 1.0])
        plt.ylim([0.0, 1.05])
        plt.xlabel("False Positive Rate")
        plt.ylabel("True Positive Rate")
        plt.title("Receiver Operating Characteristic (ROC) Curve")
        plt.legend(loc="lower right")
        plt.show()
