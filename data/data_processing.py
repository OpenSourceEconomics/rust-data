"""
This module creates a pickle file which contains the total number of observations for
each group. Therefore every DataFrame row contains the bus identifier, the state
variable and the according decision.
"""
import os

import numpy as np
import pandas as pd


def data_processing(init_dict):
    """
    This function processes data from pickle files, which have the structure as
    explained in the documentation of Rust, to a pandas DataFrame saved in a pickle
    file, which contains in each row the Bus ID, the current period, the current
    state of the bus and the decision in this period.

    :param init_dict: A dictionary containing the name of the group and the size of
    the milage bins, which is used to discretize the raw data.

    :return: The processed data as pandas dataframe.

    """
    dirname = os.path.dirname(__file__)
    df_pool = pd.DataFrame()
    groups = init_dict["groups"]
    binsize = init_dict["binsize"]
    idx = pd.IndexSlice
    for group in groups.split(","):
        df_raw = pd.read_pickle(f"{dirname}/pkl/group_data/{group}.pkl")
        num_periods = df_raw.shape[1] - 11
        bus_index = df_raw["Bus_ID"].unique().astype(int)
        period_index = np.arange(num_periods)
        index = pd.MultiIndex.from_product(
            [bus_index, period_index], names=["Bus_ID", "period"]
        )
        df = pd.DataFrame(index=index, columns=["state", "mileage", "usage"])
        df = df.assign(decision=0)
        for i, index in enumerate(df_raw.index):
            bus_row = df_raw.loc[index][11:].reset_index(drop=True)
            repl_mil = [df_raw.loc[index, "Odo_1"], df_raw.loc[index, "Odo_2"]]
            replacement_decisions = []
            if repl_mil[0] > 0:
                replacement_decisions += [np.max(bus_row[bus_row < repl_mil[0]].index)]
                if repl_mil[1] > 0:
                    bus_row[
                        (repl_mil[0] < bus_row) & (bus_row < repl_mil[1])
                    ] -= repl_mil[0]
                    replacement_decisions += [
                        np.max(bus_row[bus_row < repl_mil[1]].index)
                    ]
                    bus_row[bus_row > repl_mil[1]] -= repl_mil[1]
                else:
                    bus_row[repl_mil[0] < bus_row] -= repl_mil[0]
            bus_milage = bus_row.values
            bus_states = (bus_milage / binsize).astype(int)
            usage = np.empty(0)
            if len(replacement_decisions) > 0:
                for decision_period in replacement_decisions:
                    df.loc[idx[bus_index[i], decision_period], "decision"] = 1
                for j, rep in enumerate(replacement_decisions):
                    if j > 0:
                        start = replacement_decisions[j - 1] + 1
                    else:
                        start = 0
                    usage = np.append(
                        usage, bus_states[start + 1 : rep + 1] - bus_states[start:rep]
                    )
                    usage = np.append(usage, np.ceil((bus_milage[rep + 1]) / binsize))
                usage = np.append(
                    usage,
                    bus_states[replacement_decisions[-1] + 2 :]
                    - bus_states[replacement_decisions[-1] + 1 : -1],
                )
            else:
                usage = bus_states[1:] - bus_states[:-1]
            df.loc[bus_index[i], "usage"] = np.append(np.nan, usage)
            df.loc[bus_index[i], "state"] = bus_states
            df.loc[bus_index[i], "mileage"] = bus_milage
        df_pool = pd.concat([df_pool, df], axis=0)
    os.makedirs(dirname + "/pkl/replication_data", exist_ok=True)
    df_pool.to_pickle(f"{dirname}/pkl/replication_data/rep_{groups}_{binsize}.pkl")

    return df_pool
