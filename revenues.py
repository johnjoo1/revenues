import pandas as pd
from parse import *
import datetime
import requests
import time
import pdb

sec_headers = {"user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"}

def get_tickers_cik():
    r = requests.get('https://www.sec.gov/files/company_tickers_exchange.json')
    j = r.json()
    cik_df = pd.DataFrame(j['data'], columns = j['fields'])
    cik_df_exchange_limited = cik_df[cik_df['exchange'].isin(['Nasdaq', 'NYSE'])].copy()
    return cik_df_exchange_limited

def full_cik(cik_int):
    cik_str = str(cik_int)
    return cik_str.zfill(10)

def check_recent_statements(df_i):
    '''
    Hard coded to make sure the latest is after 2021-9-1.
    Return back to this to figure out a better way. Might be just to use the datetime.now() within 90 days.
    '''
    return df_i.sort_values('start', ascending=False)['end'].iloc[0]>datetime.datetime(2021,9,1)

def get_RevenueFromContractWithCustomerExcludingAssessedTax(cik_str):
    r = requests.get(f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik_str}/us-gaap/RevenueFromContractWithCustomerExcludingAssessedTax.json', headers=sec_headers)
    data =r.json()
    df_i = pd.DataFrame(data['units']['USD'])
    df_i['start']=pd.to_datetime(df_i['start'])
    df_i['end']=pd.to_datetime(df_i['end'])
    return df_i

def get_Revenues(cik_str):
    r = requests.get(f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik_str}/us-gaap/Revenues.json', headers=sec_headers)
    # if r.status_code == 404:
    #     r = requests.get(f'https://data.sec.gov/api/xbrl/companyconcept/CIK{cik_str}/ifrs-full/Revenue.json', headers=sec_headers)
    data =r.json()
    df_i = pd.DataFrame(data['units']['USD'])
    df_i['start']=pd.to_datetime(df_i['start'])
    df_i['end']=pd.to_datetime(df_i['end'])
    return df_i


    
def get_revenues(cik):
    cik_str = full_cik(cik)
    df_cf = get_companyfacts(cik_str)
    try:
        df_rev1 = get_RevenueFromContractWithCustomerExcludingAssessedTax(cik_str)
    except ValueError:
        df_rev1 = None
    try:
        df_rev2 = get_Revenues(cik_str)
    except ValueError:
        df_rev2=None
    df_i = pd.concat([df_rev1, df_rev2])
    df_i = df_i.sort_values(['end', 'start']).reset_index(drop=True)
    df_i['span'] = df_i['end'] - df_i['start']
    return df_i

def find_10q_between_dates(df, start, end):
    row_10qs = df[df['form'] == '10-Q']
    quarter_span = row_10qs['span'].apply(lambda x: (datetime.timedelta(days=85)) <= x <= (datetime.timedelta(days=95)))
    rows_10q_quarter = row_10qs[quarter_span]
    all_possible_rows =  rows_10q_quarter[
        (rows_10q_quarter['start']>= start) &
        (rows_10q_quarter['start']<end) &
        (rows_10q_quarter['end']<=end)
    ]
    # no duplicates
    cleaned_rows = all_possible_rows[~all_possible_rows.duplicated(subset = ['start', 'end'])]

    # make consecutive
    end = cleaned_rows['end'].iloc[0]
    start = end + datetime.timedelta(days=1)
    rows_to_drop = []
    for i,row in cleaned_rows.iloc[1:].iterrows():
        if row['start'] == start:
            start = row['end'] + datetime.timedelta(days=1)
        else:
            rows_to_drop.append(i)
    cleaned_rows = cleaned_rows.drop(rows_to_drop)

    
    if verify_no_date_gaps(cleaned_rows):
        return cleaned_rows
    else:
        raise ValueError(rows_to_drop, all_possible_rows.loc[rows_to_drop], cleaned_rows)

def verify_no_date_gaps(df):
    # input should be the output from find_10q_between_dates
    df_sorted = df.sort_values('start')
    starts = df['start'].iloc[1:].reset_index(drop=True) 
    ends = (df['end'].iloc[:-1]+datetime.timedelta(days=1)).reset_index(drop=True)
    return (starts == ends).all()

def get_clean_revenues(df_i):
    rev_quarters = []
    next_end_date = df_i.iloc[-1]['end'] # initialize condition
    for i, row in df_i[::-1].iterrows():
        quarter_span = datetime.timedelta(days=85) <= row['span'] <= datetime.timedelta(days=95)
        year_span = datetime.timedelta(days=350) <= row['span'] <= datetime.timedelta(days=375)
        form_10q = row['form'] == '10-Q'
        form_10k = row['form'] == '10-K'
        if quarter_span & form_10q & (row['end'] == next_end_date):
            rev_quarters.append([
                row['start'],
                row['end'],
                row['val'],
                row['span']
            ])
            next_end_date = row['start'] - datetime.timedelta(days=1)
        elif year_span & form_10k & (row['end']== next_end_date):
            try:
                row_10qs = find_10q_between_dates(df_i, row['start'], row['end'])
            except IndexError:
                # no rows returned. either there's a gap in history or we're at the end. 
                return pd.DataFrame(rev_quarters, columns = ['start', 'end', 'val', 'span'])
            missing_span = row['end'] - row_10qs['end'].max()
            if datetime.timedelta(days=85) <= missing_span <= datetime.timedelta(days=95):
                missing_span_start = row_10qs['end'].max() + datetime.timedelta(days=1)
                missing_span_end = row['end']
                val = row['val'] - row_10qs['val'].sum()
                rev_quarters.append([
                    missing_span_start, 
                    missing_span_end,
                    val,
                    missing_span
                ])
                next_end_date = row_10qs['end'].max()
            else:
                return pd.DataFrame(rev_quarters, columns = ['start', 'end', 'val', 'span'])
    return pd.DataFrame(rev_quarters, columns = ['start', 'end', 'val', 'span'])

def get_revs_df(cik):
    df_all_rev = get_revenues(cik)
    if not check_recent_statements(df_all_rev):
        raise ValueError('no recent statements available')
    revenues_df = get_clean_revenues(df_all_rev)
    return revenues_df



if __name__ == "__main__":
    cik_df = get_tickers_cik()
    cik_df['revenues'] =None
    cik_df['revenues'] =cik_df['revenues'].astype('object')
    for i, row in cik_df.iloc[:].iterrows():
        print(row['ticker'], i)
        try:
            # pdb.set_trace()
            cik_df.at[i,'revenues'] = get_revs_df(row['cik']).to_dict(orient='records')
        except ValueError:
            print(i, row['ticker'], "could not get data")
            pass
        time.sleep(1)

# I should use data.sec.gov/api/xbrl/companyfacts/ instead of companyconcepts. see https://www.sec.gov/edgar/sec-api-documentation
    # financial companies get revenue from 'Revenues', 'RevenuesNetOfInterestExpense'
    # foreign companies don't use gaap and report in different currency
