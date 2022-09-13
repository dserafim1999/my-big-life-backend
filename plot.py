import matplotlib.pyplot as plt
import json

def plot(x, y, xlabel, ylabel, title):
    plt.plot(x, y)  
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.show()

def plot_seconds_day(metrics):
    day, seconds = [], []
    for m in metrics:
        day.append(m["day"])
        seconds.append(m["start"])
    plot(day, seconds, 'Days Processed', 'Seconds Elapsed', 'Day Processing Time Evolution')

def plot_points_duration(metrics):
    points_dur = []
    for m in metrics:
        points_dur.append((m["points"], m["duration"]))

    points_dur.sort()
    points_dur_x = [c[0] for c in points_dur]
    points_dur_y = [c[1] for c in points_dur]

    plot(points_dur_x, points_dur_y, 'Nº Points', 'Duration (seconds)', 'Processing Time for Nº Points')

if __name__ == '__main__':
    with open('metrics.json', 'r') as metrics_file:
        metrics = json.load(metrics_file)

    plot_seconds_day(metrics)
    plot_points_duration(metrics)