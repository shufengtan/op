import pandas as pd
import numpy as np
from dataclasses import dataclass, field

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
        _df = df[df.Symbol.str.contains(r'-\W*[A-Z]+\d+[CP]\d+')]
        df_sp = _df.Symbol.str.extract( r'-\W*([A-Z]+)(\d+)([CP])(\d+)')
        df_sp.columns = ['symbol', 'expDt', 'type', 'strike']
        df_sp['expDt'] = pd.to_datetime(df_sp['expDt'], format='%y%m%d')
        df_sp['strike'] = pd.to_numeric(df_sp['strike'], errors='coerce').astype(float)
        return pd.concat([df_sp, _df.loc[:, ['Quantity', 'Last Price', 'Average Cost Basis', 'Cost Basis Total']]], axis=1)

    def capital_risks(self, df_pos):
        _dfc = df_pos[df_pos.type == 'C']
        c_risk = (_dfc.Quantity * 100 * _dfc['Average Cost Basis']).sum().item()
        _dfp = df_pos[df_pos.type == 'P']
        p_risk = (-100*_dfp.Quantity * _dfp['strike']).sum().item()
        return {'C_risk': c_risk, 'P_risk': p_risk}

if __name__ == '__main__':
    import sys
    csv_file = sys.argv[1]
    self = FidelityPositions(csv_file)
    print(self.get_option_positions())