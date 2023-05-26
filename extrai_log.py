import re
import pandas as pd
import csv
from PyPDF2 import *
from conecta_db import DB

with open(r"Z:\PCP\Log_PlasmaXPR170.txt", "r", encoding='utf-8') as text:
    lines = text.readlines()
    # print(*lines, sep='')


class Extrai_dados(object):
    def __init__(self):
        self.lines = lines

    def get_arr_name(self):
        lis_arr = [re.findall('[0-9]{4,8}.CNC', x) for x in self.lines]
        lis_arr = [item for w in lis_arr for item in w]
        lis_arr = [w[:-5] for w in lis_arr]
        return lis_arr

    def get_date_log(self):
        dates_arr = [re.findall('[a-z,A-z]{4}: [0-9]{2}-[a-z,A-z]{3}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', x) for x in
                     self.lines]
        dates_arr = [item for w in dates_arr for item in w]
        dates_arr = [w[5:] for w in dates_arr]
        return dates_arr

    def make_df(self):
        df = pd.DataFrame(list(zip(self.get_arr_name(), self.get_date_log())), columns=['arranjo', 'data_log'])
        return df


class Extrai_dados_pdf(Extrai_dados):
    def __init__(self):
        super().__init__()
        self.last = self.make_df().iloc[-1]

    ###todo trazer os tempos do Sigma para o df
    @property
    def dados_pdf(self):
        last_line = '1114426'  # self.last[0]#  1117713
        op_pdf = []
        reader = PdfReader(f"Y:\\Cnc\\Plasma_Cnc\\Pdf_Xml\\{last_line}.PDF")
        for pg in range(len(reader.pages)):
            page = reader.pages[pg]
            tex = page.extract_text().split('Programa:')
            for line in tex:
                op_pdf.append(re.findall('[0-9]{1,3} [0-9]{5,6} [0-9]{1,4} [0-9]{2,4}.[0-9]{2}'
                                         ' mm [0-9]{2,4}.[0-9]{2} mm [0-9]{6,7}\n '
                                         '[0-9]{2}:[0-9]{2}:[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}', line))
        op_pdf = [w for k in op_pdf for w in k]
        op_ = [l.split(' ') for l in op_pdf]
        df = pd.DataFrame(op_, columns=['id_pdf', 'cod_item', 'qtde', 'sai_1',
                                        'sai_2', 'sai_3', 'sai_4', 'op', 'sai_5', 'single_time'])
        df = df.drop(['sai_1', 'sai_2', 'sai_3', 'sai_4', 'sai_5'], axis=1)
        df['qtde'] = df['qtde'].astype(int)
        df['single_time'] = pd.to_datetime(df['single_time'], format='%H:%M:%S').dt.time
        df = df.groupby(['id_pdf', 'cod_item', 'op', 'single_time'], as_index=False)['qtde'].sum()
        df['op'] = df['op'].str.replace('\n', '')
        return df

    def get_focco_time(self):
        cod_item = self.dados_pdf
        cod_item = cod_item['cod_item']
        DB.tempos_focco(cod_item)
        return cod_item


de = Extrai_dados_pdf()
de.get_focco_time()
