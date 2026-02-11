import pandas as pd
import numpy as np
from dataclasses import dataclass, field
import plotly.express as px
from plotly.subplots import make_subplots

@dataclass(slots=True)
class FidelityPositions:
    csv_file: str
    df_pos: pd.DataFrame = field(init=False)
    def __post_init__(self):
        self.df_pos = self.load_fidelity_positions(self.csv_file)

    def load_fidelity_positions(self, csv_file):
        df_pos = pd.read_csv(csv_file, index_col=None)
        if np.any(df_pos.index.str.contains('X')):
            preserved_cols = df_pos.columns
            df_pos = df_pos.reset_index().iloc[:, :-1]
            df_pos.columns = preserved_cols
        df_pos = df_pos.drop(columns=[c for c in df_pos.columns if 'Gain/Loss' in c])
        df_pos = df_pos[~df_pos['Average Cost Basis'].isna() & (df_pos['Last Price'] != '--')]
        numeric_cols = ['Quantity', 'Cost Basis Total', 'Average Cost Basis', 'Last Price']
        for col in numeric_cols:
            df_pos[col] = df_pos[col].apply(lambda x: x.replace('$', '') if type(x) is str else x)
            df_pos[col] = pd.to_numeric(df_pos[col], errors='coerce')
        return df_pos

    def get_option_positions(self):
        df = self.df_pos
        _df = df[df.Symbol.str.contains(r'-\W*[A-Z]+\d+[CP][\d\.]+')]
        df_sp = _df.Symbol.str.extract( r'-\W*([A-Z]+)(\d+)([CP])([\d\.]+)')
        df_sp.columns = ['symbol', 'expDt', 'type', 'strike']
        df_sp['expDt'] = pd.to_datetime(df_sp['expDt'], format='%y%m%d')
        df_sp['strike'] = pd.to_numeric(df_sp['strike'], errors='coerce').astype(float)
        return pd.concat([df_sp, _df.loc[:, ['Quantity', 'Last Price', 'Average Cost Basis', 'Cost Basis Total']]], axis=1)

    def option_position_pies(self, df_pos):
        _df = pd.DataFrame({
            'symbol': df_pos.symbol,
            'type': df_pos.type,
            'amount': df_pos.apply(lambda r: 100*r.Quantity*r['Average Cost Basis'] if r.type=='C' else -100*r.Quantity*r.strike, axis=1)
        })
        _df = _df.groupby(['type', 'symbol']).sum().reset_index()
        risk_dict = dict(_df.groupby('type').amount.sum())
        opt_types = list(_df.type.unique())
        titles = [f'{_t} option total: {risk_dict[_t].item()}' for _t in opt_types]
        fig = make_subplots(rows=1, cols=2, subplot_titles=titles, specs=[[{'type': 'domain'}, {'type': 'domain'}]])
        for _j, opt_type in enumerate(opt_types):
            chart = px.pie(_df[_df.type==opt_type], names='symbol', values='amount')
            for trace in chart.data:
                fig.add_trace(trace, row=1, col=_j+1)
        fig.show()
        return _df

if __name__ == '__main__':
    import sys
    csv_file = sys.argv[1]
    self = FidelityPositions(csv_file)
    print(self.get_option_positions())