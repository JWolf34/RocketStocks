import datetime
import logging
import numpy as np
import pandas as pd
from rocketstocks.core.utils.dates import date_utils

logger = logging.getLogger(__name__)


class indicators:

    class volume:

        def avg_vol_at_time(data: pd.DataFrame, periods: int = 10, dt: datetime.datetime = None):
            logger.debug(f"Calculating avg_vol_at_time {dt} over last {periods} periods")
            if dt is None:
                dt = date_utils.dt_round_down(datetime.datetime.now() - datetime.timedelta(minutes=5))
            else:
                dt = date_utils.dt_round_down(dt)
            time = datetime.time(hour=dt.hour, minute=dt.minute)

            filtered_data = data[data['datetime'].apply(lambda x: x.time()) == time]
            return filtered_data['volume'].tail(periods).mean(), time

        def rvol(data: pd.DataFrame, periods: int = 10, curr_volume: float = None):
            avg_volume = data['volume'].tail(periods).mean()
            logger.debug(f"Calculating rvol over last {periods} periods")
            rvol = curr_volume / avg_volume
            logger.debug(f"avg_volume ({avg_volume}) / curr_volume ({curr_volume}) = rvol ({rvol})")
            return rvol

        def rvol_at_time(data: pd.DataFrame, today_data: pd.DataFrame, periods: int = 10, dt: datetime.datetime = None):
            avg_vol_at_time, time = indicators.volume.avg_vol_at_time(data=data, periods=periods, dt=dt)

            logger.debug(f"Calculating rvol_at_time {dt} over last {periods} periods")
            try:
                curr_vol_at_time = today_data[today_data['datetime'].apply(lambda x: x.time()) == time]['volume'].iloc[0]
                rvol = curr_vol_at_time / avg_vol_at_time
                logger.debug(f"avg_vol_at_time ({avg_vol_at_time}) / curr_vol_at_time ({curr_vol_at_time}) = rvol ({rvol})")
                return rvol
            except IndexError:
                logger.debug(f"Could not process rvol_at_time - no volume data exists at time {time}. Latest row:\n{today_data.iloc[0]}")
                return np.nan
