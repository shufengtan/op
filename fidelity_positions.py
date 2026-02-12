import pandas as pd
import numpy as np
from dataclasses import dataclass, field
import plotly.express as px
from plotly.subplots import make_subplots

@dataclass(slots=True)
class FidelityPositions:
    csv_file: str
    df_pos: pd.DataFrame = field(init=False)
    total_cash: float = 0.0
    total_value: float = 0.0
    def __post_init__(self):
        self.df_pos = self.load_fidelity_positions(self.csv_file)

    def load_fidelity_positions(self, csv_file):
        df_pos = self.read_fidelity_porfolio_position_csv_file(csv_file)
        df_pos = df_pos[~df_pos['Average Cost Basis'].isna() & (df_pos['Last Price'] != '--')]
        numeric_cols = ['Quantity', 'Cost Basis Total', 'Average Cost Basis', 'Last Price']
        for col in numeric_cols:
            df_pos[col] = df_pos[col].apply(lambda x: x.replace('$', '') if type(x) is str else x)
            df_pos[col] = pd.to_numeric(df_pos[col], errors='coerce')
        return df_pos

    def read_fidelity_porfolio_position_csv_file(self, csv_file):
        df_pos = pd.read_csv(csv_file, index_col=None)
        if np.any(df_pos.index.str.contains('X')):
            preserved_cols = df_pos.columns
            df_pos = df_pos.reset_index().iloc[:, :-1]
            df_pos.columns = preserved_cols
        df_pos = df_pos.drop(columns=[c for c in df_pos.columns if 'Gain/Loss' in c])
        df_pos = df_pos[~pd.isna(df_pos['Current Value']) & (df_pos['Current Value'] != '--')]
        df_pos['Current Value'] = pd.to_numeric(df_pos['Current Value'].apply(lambda x: x.replace('$', '')))
        df_cash = df_pos[df_pos['Last Price'].isna() & df_pos['Symbol'].str.contains(r'Pending activity|USD\*\*\*|XX\*\*')]
        self.total_cash = df_cash['Current Value'].sum()
        self.total_value = df_pos['Current Value'].sum()
        return df_pos

    def get_option_positions(self):
        df = self.df_pos
        _df = df[df.Symbol.str.contains(r'-\W*[A-Z]+\d+[CP][\d\.]+')]
        df_sp = _df.Symbol.str.extract( r'-\W*([A-Z]+)(\d+)([CP])([\d\.]+)')
        df_sp.columns = ['symbol', 'expDt', 'type', 'strike']
        df_sp['expDt'] = pd.to_datetime(df_sp['expDt'], format='%y%m%d')
        df_sp['strike'] = pd.to_numeric(df_sp['strike'], errors='coerce').astype(float)
        return pd.concat([df_sp, _df.loc[:, ['Quantity', 'Last Price', 'Average Cost Basis', 'Cost Basis Total']]], axis=1)

    def sum_sell_put_premium(self, df_pos):
        _df = df_pos[(df_pos.type=='P') & (df_pos.Quantity < 0)]
        return -100 * (_df['Quantity'] * _df['Average Cost Basis']).sum()

    def option_position_pies(self, df_pos):
        _df = pd.DataFrame({
            'symbol': df_pos.symbol,
            'strategy': df_pos.apply(lambda r: ('Sell ' if r.Quantity < 0 else 'Buy ' if r.Quantity > 0 else '? ') + r.type, axis=1),
            'amount': df_pos.apply(lambda r: 100*r.Quantity*r['Average Cost Basis'] if r.Quantity > 0 else -100*r.Quantity*r.strike, axis=1)
        })
        _df = _df.groupby(['strategy', 'symbol']).sum().reset_index()
        risk_dict = dict(_df.groupby('strategy').amount.sum())
        strat_list = sorted(_df.strategy.unique(), reverse=True)
        titles = [f'{s} option total: {risk_dict[s].item()}' for s in strat_list]
        fig = make_subplots(rows=1, cols=len(titles), subplot_titles=titles, specs=[[{'type': 'domain'}, {'type': 'domain'}]])
        for _j, strategy in enumerate(strat_list):
            chart = px.pie(_df[_df.strategy==strategy], names='symbol', values='amount')
            for trace in chart.data:
                fig.add_trace(trace, row=1, col=_j+1)
        fig.show()
        return _df

if __name__ == '__main__':
    import sys
    csv_file = sys.argv[1]
    self = FidelityPositions(csv_file)
    print(self.get_option_positions())