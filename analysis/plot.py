import math
import operator
from collections import defaultdict

import numpy
import matplotlib.pyplot as plt
from matplotlib import gridspec

from sklearn.linear_model import LinearRegression
# from sklearn.preprocessing import PolynomialFeatures

min_threshold_on_max_stage_time = 1000 # ms
number_of_data_for_learn = 4
min_scale_for_use = 5
only_analyse_bigest_job = True

def plot_all_stages(dict_of_apps_for_different_scales, name_prefix=""):
    app_dict = dict_of_apps_for_different_scales
    stages_times = defaultdict(dict)

    for scale in app_dict:
        if scale >= min_scale_for_use:
            max_job_runtime = max(map(lambda x: x.get_runtime(), app_dict[scale].jobs.values()))
            for job in app_dict[scale].jobs.values():
                if only_analyse_bigest_job:
                    if job.get_runtime() < max_job_runtime:
                        continue
                for stage in job.stages:
                    stages_times[stage.stage_id][scale] = stage.get_completion_time()

    for stage_id in list(stages_times.keys()):
        if max(stages_times[stage_id].values()) < min_threshold_on_max_stage_time:
            stages_times.pop(stage_id)

    cols = 2
    rows = int(math.ceil(len(stages_times) / cols))
    gs = gridspec.GridSpec(rows, cols)
    fig = plt.figure(figsize=(10, 5))

    errors, abs_errors, sum_actual = 0, 0, 0

    for index, stage_id in enumerate(stages_times):
        pairs = sorted(stages_times[stage_id].items(), key=operator.itemgetter(0))
        scales = numpy.array(
            [p[0] for p in pairs]
        ).reshape((-1, 1))
        times = numpy.array(
            [p[1] for p in pairs]
        )
        scales_for_learn = scales[:number_of_data_for_learn]
        times_for_learn = times[:number_of_data_for_learn]

        ax = fig.add_subplot(gs[index])
        ax.plot(scales[number_of_data_for_learn-1:], times[number_of_data_for_learn-1:], ".-", color="blue")
        ax.plot(scales[:number_of_data_for_learn], times[:number_of_data_for_learn], ".-", color="green")

        linear_regressor = LinearRegression()
        linear_regressor.fit(scales_for_learn, times_for_learn)
        times_linear_predicted = linear_regressor.predict(scales)

        ax.plot(scales, times_linear_predicted, color="red")

        # polynomial_transform_of_scales = \
        #     PolynomialFeatures(degree=3, include_bias=False).fit_transform(scales) # , include_bias=True
        # polynomial_transform_of_scales_for_learn = \
        #     PolynomialFeatures(degree=3, include_bias=False).fit_transform(scales_for_learn)
        # polynomial_regressor = LinearRegression() # fit_intercept=False
        # polynomial_regressor.fit(polynomial_transform_of_scales_for_learn, times_for_learn)
        # times_polynomial_predicted = polynomial_regressor.predict(polynomial_transform_of_scales)
        #
        # ax.plot(scales, times_polynomial_predicted, "--", color="purple")

        error = times_linear_predicted[-1] - times[-1]
        errors += error
        abs_errors += abs(error)
        sum_actual += times[-1]

        ax.set_title(f"stage_{stage_id}")

    # fig.suptitle(name_prefix + f" (errors={errors:.2f}, abs_errors={abs_errors:.2f}, sum_actual={sum_actual})")
    fig.tight_layout()
    plt.savefig(f"parser_output/_{name_prefix}_stages_analysis.pdf")
