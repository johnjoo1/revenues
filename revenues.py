import pandas as pd
from parse import *
import datetime
import requests, zipfile, io, json
import time
import pdb

sec_headers = {"user-agent":"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36"}

def bulk_download():
    r = requests.get('https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip', headers = sec_headers)
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall("bulk_download")

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

class Company:
    def __init__(self, cik):
        self.cik_str = self.full_cik(cik)
        self.get_companyfacts(self.cik_str)

    def full_cik(self, cik_int):
        cik_str = str(cik_int)
        return cik_str.zfill(10)

    def get_companyfacts(self,cik_str, production=False):
        if production ==True:
            r = requests.get(f'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik_str}.json', headers=sec_headers)
            try:
                data = r.json()
            except ValueError:
                self.accounting = None
                return None
        else:
            with open(f'bulk_download/{cik_str}.json') as f:
                data = json.load(f)
        self.data = data
        self.cik_int = data['cik']
        self.name = data['entityName']
        self.accounting =self.accounting_method()
        self.get_accounting_currency()
        self.concepts = data['facts'][self.accounting].keys()
        

    def accounting_method(self):
        keys = self.data['facts'].keys()
        non_dei_keys = [x for x in keys if x !='dei']

        if 'us-gaap' in non_dei_keys:
            return 'us-gaap'
        elif 'ifrs-full' in non_dei_keys:
            return 'ifrs-full'
        else:
            raise ValueError("unknown accounting type")

    def get_accounting_currency(self):
        first_concept = list(self.data['facts'][self.accounting].keys())[0]
        currency = list(self.data['facts'][self.accounting][first_concept]['units'].keys())[0]
        self.accounting_currency = currency
    
    def get_revenue_type(self, revenue_type):
        currency = self.accounting_currency
        if currency != 'USD':
            raise ValueError("Currency is not USD.")
        d = self.data['facts'][self.accounting][revenue_type]['units'][currency]
        df = pd.DataFrame(d)
        return df

    def get_revenues(self):
        data = self.data
        try:
            df1 = self.get_revenue_type('RevenueFromContractWithCustomerExcludingAssessedTax')
        except KeyError:
            df1 = None
        try:
            df2 = self.get_revenue_type('Revenues')
        except KeyError:
            df2 = None
        df_i = pd.concat([df1, df2])
        df_i['start']=pd.to_datetime(df_i['start'])
        df_i['end']=pd.to_datetime(df_i['end'])
        df_i = df_i.sort_values(['end', 'start']).reset_index(drop=True)
        df_i['span'] = df_i['end'] - df_i['start']
        self.combined_revenues_raw = df_i
        return df_i
        # Does df_i need to be an attribute? keep going to incorporate into Company class
        # use the bulk download
        # create some tests with the bulk download (static dataset) (can't hardocde stuff!)

    def check_recent_statements(self, df_i):
        '''
        Checks whether the latest statement was within the last 92 days.
        Returns boolean.
        '''
        last_possible_quarterly = datetime.datetime.now() - datetime.timedelta(days=92)
        return df_i.sort_values('start', ascending=False)['end'].iloc[0]>last_possible_quarterly

    def get_clean_revenues(self):
        rev_quarters = []
        next_end_date = self.combined_revenues_raw.iloc[-1]['end'] # initialize condition, starting at most recent and going backwards
        for i, row in self.combined_revenues_raw[::-1].iterrows():
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
                    row_10qs = self.find_10q_between_dates(self.combined_revenues_raw, row['start'], row['end'])
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

    def find_10q_between_dates(self, df, start, end):
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

        
        if self.verify_no_date_gaps(cleaned_rows):
            return cleaned_rows
        else:
            raise ValueError(rows_to_drop, all_possible_rows.loc[rows_to_drop], cleaned_rows)

    def verify_no_date_gaps(self, df):
        # input should be the output from find_10q_between_dates
        df_sorted = df.sort_values('start')
        starts = df['start'].iloc[1:].reset_index(drop=True) 
        ends = (df['end'].iloc[:-1]+datetime.timedelta(days=1)).reset_index(drop=True)
        return (starts == ends).all()

    def get_revs_df(self):
        self.get_revenues()
        if not check_recent_statements(self.combined_revenues_raw):
            raise ValueError('no recent statements available')
        revenues_df = self.get_clean_revenues()
        self.cleaned_revenues = revenues_df
        return revenues_df



if __name__ == "__main__":
    # cik_df = get_tickers_cik()
    # cik_df['revenues'] =None
    # cik_df['revenues'] =cik_df['revenues'].astype('object')
    # for i, row in cik_df.iloc[:].iterrows():
    #     print(row['ticker'], i)
    #     company = Company(row['cik'])
    #     if company.accounting == 'us-gaap':
    #         if company.accounting_currency == 'USD':
    #             cik_df.at[i,'revenues'] = company.get_revs_df().to_dict(orient='records')

        # try:
        #     # pdb.set_trace()
        #     cik_df.at[i,'revenues'] = get_revs_df(row['cik']).to_dict(orient='records')
        # except ValueError:
        #     print(i, row['ticker'], "could not get data")
        #     pass
        # time.sleep(1)

    # cik = cik_df[cik_df['ticker']=='AVGO']['cik'].iloc[0]
    # Company(cik).get_revs_df()

    bulk_download()


# I should use data.sec.gov/api/xbrl/companyfacts/ instead of companyconcepts. see https://www.sec.gov/edgar/sec-api-documentation
    # financial companies get revenue from 'Revenues', 'RevenuesNetOfInterestExpense'
    # foreign companies don't use gaap and report in different currency
