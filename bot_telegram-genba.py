#Создание отчета с информацией о значениях ключевых метрик за предыдущий день
#график с значениями метрик за предыдущие 7 дней
#Ключевые метрики: DAU, Просмотры, Лайки, CTR
#Автоматизация отправки отчета с помощью Airflow.
import pandas as pd
import pandahouse as ph
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
import telegram
import io

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

def ch_get_df(query='Select 1', host='#', db = '#', user='#', password='#'):
    
    connection = {'host': host,
                      'database': db,
                      'user': user, 
                      'password': password
                     }

    df = ph.read_clickhouse(query, connection=connection)
    return df

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'g.genba',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=15),
    'start_date': datetime(2022, 9, 23) #,
}

# Интервал запуска DAG
schedule_interval = '0 11 * * *'

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_genba_bot():

    @task()
    def extract_df():
        # Забираем данные
        query = '''
            SELECT 
                toDate(time) as event_date,
                uniqExact(user_id) as DAU,
                countIf(action, action = 'view') as views,
                countIf(action, action = 'like') as likes,
                round(likes/views, 3) as CTR  
            FROM 
                simulator_20220820.feed_actions 
            WHERE 
                toDate(time) between today() - interval 8 day and today() - interval 1 day
            group by 
                toDate(time) as event_date
            '''

        df = ch_get_df(query)
        return df

    # Подключение к боту
    my_token = '#' 
    bot = telegram.Bot(token=my_token)
    

    # Получение вчерашней активности пользователей
    @task
    def get_yesterday_activity(df, chat_id=-555114317):  

        yesterd = (datetime.today() - timedelta(days=1)).date()
        df_today = df[df.event_date == yesterd]

        msg_agg = f"Ежедневный отчёт:\nПользовательская активность за {yesterd}:\n \nЛайки: {df.likes[0]:_}\nПросмотры: {df.views[0]:_}\nDAU: {df.DAU[0]:_}\nCTR: {df.CTR[0]}"
        bot.sendMessage(chat_id=chat_id, text=msg_agg)
        print(msg_agg)

    # Получение недельной активности пользователей в виде графиков
    @task
    def plot_last_week_activiy(df, chat_id = -555114317):

        # Строим графики

        sns.set(rc={'figure.figsize':(30,15)})

        plt.subplot(2, 2, 1)
        sns.lineplot(data = df, x = 'event_date', y = 'likes',  marker='o', color = '#DC143C')
        plt.title('График лайков за последнюю неделю', size = 15)
        plt.ylabel('')
        plt.xlabel('')

        plt.subplot(2, 2, 2)
        sns.lineplot(data = df, x = 'event_date', y = 'views',  marker='o', color = '#DC143C')
        plt.title('График просмотров за последнюю неделю', size = 15)
        plt.ylabel('')
        plt.xlabel('')

        plt.subplot(2, 2, 3)
        sns.lineplot(data = df, x = 'event_date', y = 'DAU',  marker='o', color = '#DC143C')
        plt.title('График DAU за последнюю неделю', size = 15)
        plt.ylabel('')
        plt.xlabel('')

        plt.subplot(2, 2, 4)
        sns.lineplot(data = df, x = 'event_date', y = 'CTR',  marker='o', color = '#DC143C')
        plt.title('График CTR за последнюю неделю', size = 15)
        plt.ylabel('')
        plt.xlabel('')

        plot_object = io.BytesIO()
        plt.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'test_plot.png'
        plt.close()

        bot.sendPhoto(chat_id=chat_id, photo=plot_object)
        print('Plots were successfuly send.')

    # Отправка данных
    df = extract_df()
    get_yesterday_activity(df)
    plot_last_week_activiy(df)
    
dag_genba_bot = dag_genba_bot()
