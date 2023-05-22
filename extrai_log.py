import re
import pandas as pd
import csv
from PyPDF2 import *

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

    def tempo_sigma(self):
        last_line = '1114426'#self.last[0]
        path = rf"Y:\Cnc\Plasma_Cnc\Pdf_Xml\{last_line}.PDF"
        reader = PdfReader(path)
        with open('text.csv', 'w', encoding='utf-8') as f:
            for pg in range(len(reader.pages)):
                page = reader.pages[pg]
                tex = [page.extract_text()]
                writer = csv.writer(f)
                writer.writerow(tex)
        mieggs = pd.read_csv('text.csv')
        print(mieggs)
        for line in mieggs:
            #print(line)
            dasda = re.findall('[0-9]{5,7}', line)
            return dasda


de = Extrai_dados_pdf()
print(de.tempo_sigma())

