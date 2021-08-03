import pandas as pd
from pandas.core.indexes.datetimes import date_range
from sqlalchemy import create_engine
from datetime import timedelta, datetime


farmSymbolMap = {
    10: "SUN",
    20: "TRX",
    30: "JST",
    40: "WBTT",
    50: "WIN",
    60: "BTCST",
    70: "YFX",
    80: "NFT",
}

decimal = {
    "SUN": 18,
    "TRX": 6,
    "JST": 18,
    "WBTT": 6,
    "WIN": 6,
    "BTCST": 17,
    "YFX": 18,
    "NFT": 6,
}


def main():
    speed_raw = generate_speed_from_sql()
    week_speed = speed_for_farm_token_week(speed_raw)
    week_reward(week_speed)


def generate_speed_from_sql():
    engine = create_engine("mysql+pymysql://tron:123456@localhost:3306/old_farm_sun")
    token_status = pd.read_sql_query("select symbol,address,farm_speed,start_time,end_time,farm_token_type from token_status where farm_speed != 0", engine)
    token_status['start_time'] = pd.to_datetime(token_status['start_time'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
    token_status['end_time'] = pd.to_datetime(token_status['end_time'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai')
    token_status['farm_speed'] = pd.to_numeric(token_status['farm_speed'])
    speed_raw = token_status.pivot(index = ['start_time','end_time','symbol','address'], columns='farm_token_type', values='farm_speed')
    speed_raw.fillna(0, inplace=True)
    speed_raw.rename(columns=farmSymbolMap, inplace=True)
    indexs = speed_raw.index.to_frame()
    periods_more_than_week = speed_raw[indexs['end_time'].sub(indexs['start_time']) > timedelta(weeks=1)]
    for index, row in periods_more_than_week.iterrows():
        date_range = pd.date_range(indexs.loc[index, 'start_time'], indexs.loc[index, 'end_time'], freq='7D')
        split_time = pd.DataFrame(zip(date_range, date_range[1:]), columns=['start_time', 'end_time'])
        df_row = periods_more_than_week.loc[[index]].reset_index(['start_time', 'end_time'], drop=True).reset_index()
        new_row = df_row.merge(split_time, how='cross').set_index(periods_more_than_week.index.names)
        speed_raw = speed_raw.append(new_row).drop(index=index)
    speed_raw.sort_index(level=['start_time', 'symbol'], inplace=True)
    speed_raw.to_csv("speed_raw.csv")
    return speed_raw


def generate_speed_from_excel():
    pass


def read_speed_raw():
    return pd.read_csv("speed_raw.csv").set_index(['start_time','end_time','symbol','address'])


def get_farm_tokens(speed_raw):
    return speed_raw.columns.to_list()


def speed_for_farm_token_week(speed_raw):
    week_speed = speed_raw.groupby(['start_time', 'end_time']).sum() * 7
    week_speed['start'] = week_speed.index.get_level_values('start_time').strftime(r"%Y%m%d").astype('str')
    week_speed['end'] = week_speed.index.get_level_values('end_time').strftime(r"%Y%m%d").astype('str')
    week_speed['period'] = week_speed[['start', 'end']].agg('-'.join, axis=1)
    week_speed = week_speed.set_index('period', drop=True).drop(columns = ['start', 'end'])
    week_speed.to_csv("week_speed.csv")
    return week_speed


def week_reward(week_speed):
    reward_for_farm_tokens = {}
    start = datetime.strptime(week_speed.index.get_level_values('period')[0].split('-')[1], r"%Y%m%d")
    end = datetime.strptime(week_speed.index.get_level_values('period')[-1].split('-')[1], r"%Y%m%d") + timedelta(weeks=23)
    date_range = pd.date_range(start, end, freq='7D')
    template = pd.DataFrame(index=week_speed.index, columns=date_range)
    for col, series in week_speed.items():
        df_reward = template.copy(deep=True)
        for index, speed in series.items():
            reward_per_week = speed / 24
            first_week = datetime.strptime(index.split('-')[1], r"%Y%m%d")
            last_week = first_week + timedelta(weeks=23)
            df_reward.loc[index, first_week:last_week] = reward_per_week
        df_reward = df_reward.append(df_reward.sum(axis=0).rename("SUM"))
        df_reward = df_reward.rename(columns=lambda x: x.strftime(r"%Y%m%d"))
        reward_for_farm_tokens[col] = df_reward
    with pd.ExcelWriter('theoretical_airdrop.xlsx') as writer:
        for key, value in reward_for_farm_tokens.items():
            value.to_excel(writer, sheet_name=key)


if __name__ == "__main__":
    main()