import numpy
from sklearn.linear_model import LinearRegression
from collections import defaultdict
import matplotlib.pyplot as plt
from matplotlib import gridspec
import math
import operator

min_threshold_on_max_stage_time = 250 # ms
number_of_data_for_learn = 5

def plot_all_stages(dict_of_apps_for_different_scales, name_prefix=""):
    app_dict = dict_of_apps_for_different_scales
    stages_times = defaultdict(dict)

    for scale in app_dict:
        for job in app_dict[scale].jobs.values():
            for stage in job.stages:
                stages_times[stage.stage_id][scale] = stage.get_tasks_average_completion_times()

    for stage_id in list(stages_times.keys()):
        if max(stages_times[stage_id].values()) < min_threshold_on_max_stage_time:
            stages_times.pop(stage_id)

    cols = 3
    rows = int(math.ceil(len(stages_times) / cols))
    gs = gridspec.GridSpec(rows, cols)
    fig = plt.figure()

    for index, stage_id in enumerate(stages_times):
        pairs = sorted(stages_times[stage_id].items(), key=operator.itemgetter(0))
        scales = numpy.array(
            [p[0] for p in pairs]
        ).reshape(-1, 1)
        times = numpy.array(
            [p[1] for p in pairs]
        ).reshape(-1, 1)

        linear_regressor = LinearRegression()
        linear_regressor.fit(scales[:number_of_data_for_learn], times[:number_of_data_for_learn])
        times_predicted = linear_regressor.predict(scales)

        ax = fig.add_subplot(gs[index])
        ax.plot(scales[number_of_data_for_learn-1:], times[number_of_data_for_learn-1:], ".-", color="blue")
        ax.plot(scales[:number_of_data_for_learn], times[:number_of_data_for_learn], ".-", color="green")
        ax.plot(scales, times_predicted, color="red")
        ax.set_title(f"stage_{stage_id}")

    fig.suptitle(name_prefix)
    fig.tight_layout()
    plt.show()

