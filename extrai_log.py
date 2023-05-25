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

    def dados_pdf(self):
        last_line = '1114426'  # self.last[0]
        op_pdf = []
        filtred_op = []
        reader = PdfReader(f"Y:\\Cnc\\Plasma_Cnc\\Pdf_Xml\\{last_line}.PDF")
        for pg in range(len(reader.pages)):
            page = reader.pages[pg]
            tex = page.extract_text().split('\n')
            for line in tex:
                op_pdf.append(re.findall(
                    '[0-9]{1,3} [0-9]{5,6} [0-9]{1,4} [0-9]{2,4}.[0-9]{2} mm [0-9]{2,4}.[0-9]{2} mm [0-9]{6,7}', line))
        op_pdf = [*set(w for k in op_pdf for w in k)]
        op_ = [l.split(' ') for l in op_pdf]
        for sublist in op_:
            is_float = lambda x: isinstance(x, float)
            float_list = list(map(lambda x: float(x) if "." in x else x, sublist))
            op_ = list(filter(lambda x: not is_float(x), float_list))
            filtred_op.append(op_)
        df = pd.DataFrame(filtred_op, columns=['id_pdf', 'cod_item', 'qtde', 'sai', 'out', 'op'])
        df = df.drop(['sai', 'out'], axis=1).astype(int)
        df = df.groupby(['id_pdf', 'cod_item', 'op'])['qtde'].sum()
        print(df[6])
        return df

    def get_focco_time(self):
        cod_item = self.dados_pdf()
        #cod_item = cod_item['cod_item']
        return cod_item


de = Extrai_dados_pdf()
print(de.get_focco_time())
