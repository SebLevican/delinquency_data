import pymysql
import numpy as np
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import warnings
from dateutil.relativedelta import relativedelta

class GetScoring:
    
    def __init__(self):
        self.getbase()
        
        self.fua = self.fetch_data(self.last_payment_date)
        self.saf = self.fetch_data(self.total_payment_amount)
        self.cs = self.fetch_data(self.credit_score)
        self.fuafs = self.fetch_data(self.uploaded_invoices)
        self.t = self.rates.set_index("id").to_dict(orient='index')
        
    def getbase(self):
        # Configurar la conexión a la base de datos
        connection = pymysql.connect(host='localhost',
                                     user='usuario',
                                     password='contraseña',
                                     database='nombre_base_de_datos',
                                     cursorclass=pymysql.cursors.DictCursor)

        # Cargar datos de diferentes tablas
        with connection.cursor() as cursor:
            cursor.execute('SELECT * FROM ia_factorings_history')
            self.dfc = pd.DataFrame(cursor.fetchall())

            cursor.execute('SELECT * FROM ia_factorings_history_prod')
            self.dfb = pd.DataFrame(cursor.fetchall())

            cursor.execute('SELECT id, company_id, debtor_id, number FROM invoices')
            invoices = pd.DataFrame(cursor.fetchall())
            invoices['debtor_id'] = invoices['debtor_id'].astype(str)
            invoices['company_id'] = invoices['company_id'].astype(str)
            invoices['key'] = invoices['company_id'] + "-" + invoices['number'] + "-" + invoices['debtor_id']
            invoices.drop_duplicates(subset='key', inplace=True)
            self.invoices = invoices.set_index('id').to_dict(orient='index')

            cursor.execute('SELECT risk_report.company_id as company_id, risk_report_detail.value as value FROM risk_report_detail LEFT JOIN risk_report ON risk_report.id = risk_report_detail.risk_report_id WHERE scoring_variables_id = 16')
            self.credit_score = pd.DataFrame(cursor.fetchall())
            self.credit_score = self.credit_score.drop_duplicates(subset='company_id')

            cursor.execute('SELECT A.invoice_id as invoice_id, SUM(A.payment_amount) + SUM(waiver) as total_payment_amount FROM payment_invoices A WHERE A.deleted_at IS NULL GROUP BY A.invoice_id')
            self.total_payment_amount = pd.DataFrame(cursor.fetchall())

            cursor.execute('SELECT invoice_id as invoice_id, MAX(a.payment_date) as date, SUM(a.payment_amount) + SUM(a.waiver) as total_payment_amount FROM payment_invoices a WHERE a.deleted_at IS NULL GROUP BY invoice_id')
            self.last_payment_date = pd.DataFrame(cursor.fetchall())

            cursor.execute('''SELECT ifh.invoice_id as id,
                                    ifh.application_date,
                                    ifh.expected_repayment_date,
                                    af.payment_date as date,
                                    i.emission_date as emission,
                                    SUM(af.payment_amount) + SUM(af.waiver) as total_payment_amount
                              FROM ia_factorings_history_prod ifh
                              LEFT JOIN invoices i ON ifh.invoice_id = i.invoice_upload_id 
                              LEFT JOIN payment_invoices af ON i.id = af.invoice_id 
                              WHERE af.deleted_at IS NULL
                              GROUP BY id''')
            self.uploaded_invoices = pd.DataFrame(cursor.fetchall())

            cursor.execute('''SELECT ifhp.invoice_id as id,
                                    fs.receiver_rut,
                                    i.amount,
                                    pf.amount_to_finance,
                                    p.rate / 12 / 100 as rate
                              FROM ia_factorings_history_prod ifhp
                              LEFT JOIN uploaded_invoices fs ON ifhp.invoice_id = fs.id
                              LEFT JOIN invoices i ON i.invoice_upload_id = fs.id
                              LEFT JOIN project_invoices pf ON i.project_invoice_id = pf.id
                              LEFT JOIN projects p ON pf.project_id = p.id''')
            self.rates = pd.DataFrame(cursor.fetchall())

        connection.close()

        self.rates = self.rates.drop_duplicates(subset='id', keep='first')

    def fetch_data(self, data_dict):
        columns = ['id', 'rut', 'company_id', 'invoice_id']
        val = None
        for column in columns:
            try:
                val = data_dict.set_index(column).to_dict(orient='index')
                break
            except KeyError:
                print('Key not found')
        return val

    def get_delay(self, row):
        due_date = row['due_date']
        paid = row['paid']
        current_date = pd.Timestamp(datetime.now().date())

        if pd.isna(paid):
            return (due_date - current_date).days
        else:
            return (due_date - paid).days

    def get_status(self, p_delay_days, p_balance):
        if p_delay_days >= 0 and p_balance >= 0:
            return "1- On time"
        elif p_delay_days >= 0 and p_balance <= 0:
            return "2- Active"
        elif p_delay_days <= 0 and p_balance >= 0:
            return "1- On time"
        elif -30 <= p_delay_days <= -1:
            return "3- Less than 30"
        elif -60 <= p_delay_days <= -31:
            return "4- 31 to 60"
        elif -90 <= p_delay_days <= -61:
            return "5- 61 to 90"
        elif -120 <= p_delay_days <= -91:
            return "6- 91 to 120"
        elif -180 <= p_delay_days <= -121:
            return "7- 121 to 180"
        elif p_delay_days <= -181:
            return "8- More than 180"

    def get_payment_age(self, row):
        balance = row['balance']
        start = row['start']
        end = row['paid']

        if pd.isna(end):
            end = pd.Timestamp(datetime.now().date())

        start_dt = pd.Timestamp(start)
        end_dt = pd.Timestamp(end)

        months_diff = (end_dt.year - start_dt.year) * 12 + (end_dt.month - start_dt.month)

        if pd.isna(balance) or balance < 0:
            return None
        else:
            return months_diff

    def get_age(self, row):
        start = row['funding_date']
        now = pd.Timestamp(datetime.now().date())
        first_cut = row['funding_date'] + pd.offsets.MonthEnd(0)
        month_diff = relativedelta(now, first_cut)
        months = month_diff.months + 12 * month_diff.years

        if now < first_cut:
            return 0
        elif months == 0:
            return 1
        else:
            return months

    def calculate_delay_ages(self, row):
        now = pd.Timestamp(datetime.now())

        if row['PERIODS_TO_COME'] == 0:
            return pd.Series({
                'delay_1_age': np.nan,
                'delay_30_age': np.nan,
                'delay_60_age': np.nan,
                'delay_90_age': np.nan,
                'delay_120_age': np.nan,
                'delay_180_age': np.nan,
                'delay_360_age': np.nan
            })

        if row['due_date'] > now:
            return pd.Series({
                'delay_1_age': np.nan,
                'delay_30_age': np.nan,
                'delay_60_age': np.nan,
                'delay_90_age': np.nan,
                'delay_120_age': np.nan,
                'delay_180_age': np.nan,
                'delay_360_age': np.nan
            })

        if row['expected_repayment_age'] == 0:
            repayment_age = 1
        else:
            repayment_age = row['expected_repayment_age']

        if pd.isnull(row['PERIODS_TO_COME']):
            row['PERIODS_TO_COME'] = self.months_between_dates(row['expected_repayment_date'], now)

        delay_1_age = repayment_age

        if row['expected_repayment_age'] == 0 and row['delay_total'] > -30 and pd.isnull(row['final_repayment_date']):
            delay_1_age = 1

        if row['PERIODS_TO_COME'] >= 2 and row['delay_total'] <= -30:
            delay_30_age = delay_1_age + 1 if row['due_date'] <= now else np.nan
        else:
            delay_30_age = np.nan

        if row['PERIODS_TO_COME'] >= 3 and row['delay_total'] <= -60:
            delay_60_age = delay_30_age + 1 if row['due_date'] <= now else np.nan
        else:
            delay_60_age = np.nan

        if row['PERIODS_TO_COME'] >= 4 and row['delay_total'] <= -90:
            delay_90_age = delay_60_age + 1 if row['due_date'] <= now else np.nan
        else:
            delay_90_age = np.nan

        if row['PERIODS_TO_COME'] >= 5 and row['delay_total'] <= -120:
            delay_120_age = delay_90_age + 1 if row['due_date'] <= now else np.nan
        else:
            delay_120_age = np.nan

        if row['PERIODS_TO_COME'] >= 6 and row['delay_total'] <= -180:
            delay_180_age = delay_120_age + 1 if row['due_date'] <= now else np.nan
        else:
            delay_180_age = np.nan

        if row['PERIODS_TO_COME'] >= 7 and row['delay_total'] <= -360:
            delay_360_age = delay_180_age + 4 if row['due_date'] <= now else np.nan
        else:
            delay_360_age = np.nan

        return pd.Series({
            'delay_1_age': delay_1_age,
            'delay_30_age': delay_30_age,
            'delay_60_age': delay_60_age,
            'delay_90_age': delay_90_age,
            'delay_120_age': delay_120_age,
            'delay_180_age': delay_180_age,
            'delay_360_age': delay_360_age
        })
    
    def get_conexion(self):
        # Set database credentials.
        creds = {
            'usr': **,
            'pwd': **,
            'hst': **,
            'prt': **,
            'dbn': **
        }
        # MySQL conection string.
        connstr = 'mysql+mysqlconnector://{usr}:{pwd}@{hst}:{prt}/{dbn}'
        # Create sqlalchemy engine for MySQL connection.
        engine = create_engine(connstr.format(**creds))
        return engine
    
    def uploaddb(self,df):

        engine = self.get_conexion()

        df.to_sql('ia_factorings_history', con=engine, if_exists='replace', index=False,)
        print('Data uploaded')

# Uso del código:
if __name__ == "__main__":
    scoring = GetScoring()
    data = scoring.getbase()
    scoring.uploaddb(data)
