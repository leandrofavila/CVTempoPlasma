import cx_Oracle
import pandas as pd


class DB:
    def __init__(self, carregamento, ops, cod_item):
        self.carregamento = carregamento
        self.ops = ops
        self.cod_item = cod_item

    @staticmethod
    def connection():
        dsn = cx_Oracle.makedsn("10.40.3.10", 1521, service_name="f3ipro")
        connection = cx_Oracle.connect(user=r"focco_consulta", password=r'consulta3i08', dsn=dsn, encoding="UTF-8")
        cur = connection.cursor()
        return cur

    def car(self):
        cur = self.connection()
        cur.execute(
            r"SELECT DISTINCT TPL.COD_ITEM,  "
            r"TOR.NUM_ORDEM, "
            r"TOR.QTDE "
            r"FROM FOCCO3I.TITENS_PLANEJAMENTO TPL "
            r"INNER JOIN FOCCO3I.TITENS_EMPR EMP      ON TPL.ITEMPR_ID = EMP.ID "
            r"INNER JOIN FOCCO3I.TITENS TIT           ON EMP.ITEM_ID = TIT.ID  "
            r"INNER JOIN FOCCO3I.TORDENS TOR          ON TPL.ID = TOR.ITPL_ID "
            r"INNER JOIN FOCCO3I.TDEMANDAS TDE        ON TOR.ID = TDE.ORDEM_ID "
            r"INNER JOIN FOCCO3I.TORDENS_ROT ROT      ON TOR.ID = ROT.ORDEM_ID "
            r"INNER JOIN FOCCO3I.TORD_ROT_FAB_MAQ FAB ON ROT.ID = FAB.TORDEN_ROT_ID "
            r"INNER JOIN FOCCO3I.TMAQUINAS MAQ        ON FAB.MAQUINA_ID = MAQ.ID "
            r"INNER JOIN FOCCO3I.TITENS_PLAN_FUNC PLA ON TPL.ID = PLA.ITPL_ID "
            r"INNER JOIN FOCCO3I.TFUNCIONARIOS TFUN   ON PLA.FUNC_ID = TFUN.ID "
            r"WHERE TOR.ID IN( "
            r"                SELECT TOR.ID "
            r"                FROM FOCCO3I.TORDENS TOR "
            r"                INNER JOIN FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC      ON TOR.ID = VINC.ORDEM_ID "
            r"                INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR  ON VINC.CARERGAM_ID = CAR.ID "
            r"                AND CAR.CARREGAMENTO IN (" + self.carregamento + ") "
            r") "
            r"AND MAQ.DESCRICAO LIKE '%PUNCIONADEIRA%' "
            r"GROUP BY TPL.COD_ITEM, TOR.NUM_ORDEM, TOR.QTDE "
        )
        car = cur.fetchall()
        car = pd.DataFrame(car, columns=['COD_ITEM', 'NUM_ORDEM', 'QTDE'])
        car['QTDE'] = car['QTDE'].astype(int)
        return car

    def thickness(self):
        lista_itens = []
        df = self.car()
        for itens in df.COD_ITEM:
            lista_itens.append(str(itens))
        lista_itens = ','.join(lista_itens)
        cur = self.connection()
        cur.execute(
            r"SELECT DISTINCT TPDM.CONTEUDO_ATRIBUTO, TIT.COD_ITEM "
            r"FROM FOCCO3I.TITENS_EMPR TEMP "
            r",FOCCO3I.TROTEIRO TEIRO "
            r",FOCCO3I.TOPERACAO TOP "
            r",FOCCO3I.TITENS TIT "
            r",FOCCO3I.TITENS_PDM TPDM "
            r",FOCCO3I.TITENS_PDM TPDM2 "
            r",FOCCO3I.TITENS_PDM TPDM3 "
            r",FOCCO3I.TITENS_PDM TPDM4 "
            r",FOCCO3I.TATRIBUTOS TAB "
            r",FOCCO3I.TATRIBUTOS TAB2 "
            r",FOCCO3I.TATRIBUTOS TAB3 "
            r",FOCCO3I.TATRIBUTOS TAB4 "
            r",FOCCO3I.TITENS_PLANEJAMENTO TPL "
            r",FOCCO3I.TORDENS TOR "
            r",FOCCO3I.TFUNCIONARIOS TFUN "
            r"WHERE TEMP.ID = TEIRO.ITEMPR_ID "
            r"AND TIT.ID = TEMP.ITEM_ID "
            r"AND TIT.ID = TPDM.ITEM_ID "
            r"AND TIT.ID = TPDM2.ITEM_ID "
            r"AND TIT.ID = TPDM3.ITEM_ID "
            r"AND TIT.ID = TPDM4.ITEM_ID "
            r"AND TEMP.ID = TPL.ITEMPR_ID "
            r"AND TPL.ID = TOR.ITPL_ID "
            r"AND TPDM.ATRIBUTO_ID = TAB.ID "
            r"AND TPDM2.ATRIBUTO_ID = TAB2.ID "
            r"AND TPDM3.ATRIBUTO_ID = TAB3.ID "
            r"AND TPDM4.ATRIBUTO_ID = TAB4.ID "
            r"AND TOR.FUNC_ID = TFUN.ID "
            r"AND TAB.DESCRICAO LIKE '%ESPESSU%' "
            r"AND TAB2.DESCRICAO LIKE '%MEDIDA X%' "
            r"AND TAB3.DESCRICAO LIKE '%MEDIDA Y%' "
            r"AND TAB4.DESCRICAO LIKE '%MEDIDA Z%' "
            r"AND TEIRO.OPERACAO_ID = TOP.ID "
            r"AND TIT.COD_ITEM IN (" + lista_itens + ") "
            r"AND TIT.DESC_TECNICA NOT LIKE '%NAO USAR%' "
        )
        thickness = cur.fetchall()
        nlis = {}
        for thic in thickness:
            nlis[thic[1]] = thic[0]
        return nlis

    def car_op(self):
        cur = self.connection()
        cur.execute(
            r"SELECT CAR.CARREGAMENTO "
            r"FROM FOCCO3I.TORDENS TOR "
            r"INNER JOIN FOCCO3I.TITENS_PLANEJAMENTO TPL  ON TPL.ID = TOR.ITPL_ID "
            r"INNER JOIN FOCCO3I.TSRENG_ORDENS_VINC_CAR VINC      ON TOR.ID = VINC.ORDEM_ID "
            r"INNER JOIN FOCCO3I.TSRENGENHARIA_CARREGAMENTOS CAR  ON VINC.CARERGAM_ID = CAR.ID "
            r"WHERE TOR.NUM_ORDEM IN ("+str(self.ops)+") "
        )
        possiveis_carr = cur.fetchall()

    def tempos_focco(self):
        cod_item = self.cod_item
        cod_item = ', '.join(cod_item)
        print(cod_item)
        cur = self.connection
        cur.execute(
            r"SELECT EMP.COD_ITEM, SUM(TROT.TEMPO) "
            r"FROM FOCCO3I.TROTEIRO TROT "
            r",FOCCO3I.TITENS_EMPR EMP "
            r"WHERE TROT.ITEMPR_ID = EMP.ID "
            r"AND EMP,COD_ITEM IN ("+cod_item+") "
            r"GROUP BY EMP.COD_ITEM "
        )

