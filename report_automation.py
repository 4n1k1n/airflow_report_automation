#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from datetime import datetime, timedelta
import pandas as pd
import requests
import pandahouse as ph
import numpy as np
import datetime as dt
import io
from dotenv import load_dotenv
import os
import telegram
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

import seaborn as sns
sns.set_style("whitegrid") 

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'n.anikin',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 4, 19),
}
# Интервал запуска DAG
schedule_interval = '0 11 * * *'

# Вход в Сlickhouse для запросов
load_dotenv()
connection = {
    'host': 'https://' + os.getenv('DB_HOST'),
    'password': os.getenv('DB_PASSWORD'),
    'user': os.getenv('DB_USER'),
    'database': os.getenv('DB_NAME')
}

my_token = os.getenv('BOT_REPORT_TOKEN')
chat_id = os.getenv('CHAT_ID')
bot = telegram.Bot(token=my_token) # получаем доступ

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_n_anikin_tg_send_report():

    @task()
    def extract_users_by_apps():
        query = '''
        SELECT uniqExact(user_id) AS users,
              'feed' AS app,
              toDate(time) AS date
        FROM simulator_20240320.feed_actions
        WHERE
            date >= today() - 31 AND
            date <= today() - 1
        GROUP BY app, date
        
        UNION ALL
        
        SELECT uniqExact(user_id) AS users,
                        'message' AS app,
                        toDate(time) as date
        FROM simulator_20240320.message_actions
        WHERE
            date >= today() - 31 AND
            date <= today() - 1
        GROUP BY app, date'''
        
        users_by_apps = ph.read_clickhouse(query=query, connection=connection)
        return users_by_apps

    @task()
    def extract_new_users():
        query = '''
        SELECT
            reg_date,
            uniqExact(user_id) AS new_users,
            source
        FROM
            (SELECT
                date,
                user_id,
                source,
                MIN(date) OVER(PARTITION BY user_id) AS reg_date
            FROM
                (SELECT
                    DISTINCT
                    toDate(time) AS date,
                    user_id,
                    source
                FROM simulator_20240320.feed_actions
                
                UNION ALL
                
                SELECT
                    DISTINCT
                    toDate(time) AS date,
                    user_id,
                    source
                FROM simulator_20240320.message_actions) t) t2
        WHERE
            reg_date >= today() - 31 AND
            reg_date <= today() - 1
        GROUP BY reg_date, source
        ORDER BY reg_date
        '''
        new_users = ph.read_clickhouse(query=query, connection=connection)
        return new_users

    @task()
    def extract_retention():
        query = '''
        SELECT
            day,
            source,
            ROUND(AVG(cohort_retention), 4) AS retention
        FROM
            (SELECT
                active_users,
                reg_date,
                date,
                ROUND((active_users / max_users), 4) AS cohort_retention,
                dateDiff('day', toDate(reg_date), toDate(date)) AS day,
                source
            FROM
                (SELECT
                    COUNT(user_id) AS active_users,
                    reg_date,
                    date,
                    MAX(active_users) OVER(PARTITION BY reg_date, source) AS max_users,
                    source
                FROM
                    (SELECT
                        date,
                        user_id,
                        source,
                        MIN(date) OVER(PARTITION BY user_id) AS reg_date
                    FROM
                        (SELECT
                            DISTINCT
                            toDate(time) AS date,
                            user_id,
                            source
                        FROM simulator_20240320.feed_actions
                        
                        UNION ALL
                        
                        SELECT
                            DISTINCT
                            toDate(time) AS date,
                            user_id,
                            source
                        FROM simulator_20240320.message_actions) t1) t2
                WHERE reg_date >= today() - 20
                GROUP BY reg_date, date, source) t3) t4
        GROUP BY day, source
        ORDER BY day, source
        '''
        retention = ph.read_clickhouse(query=query, connection=connection)
        return retention
        
    @task()
    def extract_new_gone_retained():
        query = '''
        SELECT
            -uniqExact(user_id) AS users,
            current_week,
            previous_week,
            status
        FROM
            (SELECT
                user_id,
                groupUniqArray(toMonday(time)) AS weeks_visited,
                addWeeks(arrayJoin(weeks_visited), +1) AS current_week,
                if(has(weeks_visited, current_week) = 1, 'retained', 'gone') AS status,
                addWeeks(current_week, -1) AS previous_week
            FROM simulator_20240320.feed_actions
            GROUP BY user_id) t1
        WHERE status = 'gone'
        GROUP BY current_week, previous_week, status
        HAVING current_week != addWeeks(toMonday(today()), +1)
        
        UNION ALL
        
        SELECT
            toInt64(uniqExact(user_id)) AS users,
            current_week,
            previous_week,
            status
        FROM
            (SELECT
                user_id,
                groupUniqArray(toMonday(time)) AS weeks_visited,
                arrayJoin(weeks_visited) AS current_week,
                addWeeks(current_week, -1) AS previous_week,
                if(has(weeks_visited, previous_week) = 1, 'retained', 'new') AS status
            FROM
                simulator_20240320.feed_actions
            GROUP BY user_id) t2
        GROUP BY current_week, previous_week, status
        '''
        
        new_gone_retained = ph.read_clickhouse(query=query, connection=connection)
        new_gone_retained['current_week'] = new_gone_retained['current_week'].apply(lambda x: x.strftime("%Y-%m-%d"))
        new_gone_retained = new_gone_retained.pivot(index='current_week', values='users', columns='status').fillna(0)
        return new_gone_retained

    @task()
    def extract_new_gone_retained_messenger():
        query = '''
        SELECT
            -uniqExact(user_id) AS users,
            current_week,
            previous_week,
            status
        FROM
            (SELECT
                user_id,
                groupUniqArray(toMonday(time)) AS weeks_visited,
                addWeeks(arrayJoin(weeks_visited), +1) AS current_week,
                if(has(weeks_visited, current_week) = 1, 'retained', 'gone') AS status,
                addWeeks(current_week, -1) AS previous_week
            FROM simulator_20240320.message_actions
            GROUP BY user_id) t1
        WHERE status = 'gone'
        GROUP BY current_week, previous_week, status
        HAVING current_week != addWeeks(toMonday(today()), +1)
        
        UNION ALL
        
        SELECT
            toInt64(uniqExact(user_id)) AS users,
            current_week,
            previous_week,
            status
        FROM
            (SELECT
                user_id,
                groupUniqArray(toMonday(time)) AS weeks_visited,
                arrayJoin(weeks_visited) AS current_week,
                addWeeks(current_week, -1) AS previous_week,
                if(has(weeks_visited, previous_week) = 1, 'retained', 'new') AS status
            FROM
                simulator_20240320.message_actions
            GROUP BY user_id) t2
        GROUP BY current_week, previous_week, status
        '''
        
        new_gone_retained_messenger = ph.read_clickhouse(query=query, connection=connection)
        new_gone_retained_messenger['current_week'] = new_gone_retained_messenger['current_week'].apply(lambda x: x.strftime("%Y-%m-%d"))
        new_gone_retained_messenger = new_gone_retained_messenger.pivot(index='current_week', values='users', columns='status').fillna(0)
        return new_gone_retained_messenger
        
    @task()
    def send_users_plots(users_by_apps, new_users, new_gone_retained, new_gone_retained_messenger):
        fig, axes = plt.subplots(4, 1, figsize=(7, 12))
        fig.suptitle('Аудитория\n', size='x-large', weight='bold')
        plot1, plot2, plot3, plot4 = range(4)
        
        # 1
        axes[plot1].title.set_text('DAU за последний месяц')
        
        sns.lineplot(x=users_by_apps['date'],
                     y=users_by_apps['users'],
                     hue=users_by_apps['app'],
                     palette='mako',
                     ax=axes[plot1])
        plt.setp(axes[plot1].get_xticklabels(), rotation=40)
        
        axes[plot1].set(xlim=(users_by_apps['date'].min(),
                              users_by_apps['date'].max()),
                        xlabel=None,
                        ylim=(0, None),
                        ylabel=None)
        
        axes[plot1].xaxis.set_major_locator(mdates.DayLocator(interval=2))
        axes[plot1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        axes[plot1].yaxis.set_major_formatter(ticker.EngFormatter())
        
        handles, labels = axes[plot1].get_legend_handles_labels()
        for i in range(len(labels)):
            if labels[i] == "feed":
                labels[i] = 'Лента новостей'
            else:
                labels[i] = 'Мессенджер'
        axes[plot1].legend(handles, labels, title = None)
        
        # 2
        axes[plot2].title.set_text('Новые пользователи ленты и мессенджера')
        sns.lineplot(x=new_users['reg_date'],
                     y=new_users['new_users'],
                     hue=new_users['source'],
                     palette='mako',
                     ax=axes[plot2])
        
        axes[plot2].set(xlim=(new_users['reg_date'].min(),
                              new_users['reg_date'].max()),
                        xlabel=None,
                        ylim=(0, None),
                        ylabel=None)
        plt.setp(axes[plot2].get_xticklabels(), rotation=40)
        
        axes[plot2].xaxis.set_major_locator(mdates.DayLocator(interval=2))
        axes[plot2].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        axes[plot2].yaxis.set_major_formatter(ticker.EngFormatter())
        
        handles, labels = axes[plot2].get_legend_handles_labels()
        for i in range(len(labels)):
            if labels[i] == "ads":
                labels[i] = 'Реклама'
            else:
                labels[i] = 'Органика'
        axes[plot2].legend(handles, labels, title = 'Источник трафика:')
        
        # 3
        axes[plot3].title.set_text('Аудитория по неделям ленты новостей')
        new_gone_retained.plot(kind='bar', stacked=True, color=['#2e1e3b', '#37659e', '#40b7ad'], ax=axes[plot3], rot=45)
        
        axes[plot3].set(xlabel=None, ylabel=None)
        axes[plot3].yaxis.set_major_formatter(ticker.EngFormatter())
        
        handles, labels = axes[plot3].get_legend_handles_labels()
        for i in range(len(labels)):
            if labels[i] == 'gone':
                labels[i] = 'Ушедшие'
            elif labels[i] == 'new':
                labels[i] = 'Новые'
            else:
                labels[i] = 'Старые'
        axes[plot3].legend(handles, labels, title=None)
        
        # 4
        axes[plot4].title.set_text('Аудитория по неделям мессенджера')
        new_gone_retained_messenger.plot(kind='bar', stacked=True, color=['#2e1e3b', '#37659e', '#40b7ad'], ax=axes[plot4], rot=45)
        
        axes[plot4].set(xlabel=None, ylabel=None)
        axes[plot4].yaxis.set_major_formatter(ticker.EngFormatter())
        
        handles, labels = axes[plot4].get_legend_handles_labels()
        for i in range(len(labels)):
            if labels[i] == 'gone':
                labels[i] = 'Ушедшие'
            elif labels[i] == 'new':
                labels[i] = 'Новые'
            else:
                labels[i] = 'Старые'
        axes[plot4].legend(handles, labels, title=None)
        
        fig.tight_layout()
        
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    @task()
    def send_users_info(users_by_apps, new_users, new_gone_retained, retention, new_gone_retained_messenger):
        last_day = users_by_apps["date"].max().strftime("%d-%m-%Y")
        week_ago_plus = (users_by_apps["date"].max() - pd.to_timedelta(6, unit='d')).strftime("%d-%m-%Y")
        week_ago = (users_by_apps["date"].max() - pd.to_timedelta(7, unit='d')).strftime("%d-%m-%Y")
        two_week_ago = (users_by_apps["date"].max() - pd.to_timedelta(13, unit='d')).strftime("%d-%m-%Y")

        message = f'''👨Аудитория👩

📰Лента новостей
DAU {last_day}: {users_by_apps.query('app == "feed" & date == @last_day')['users'].values[0]}
DAU {week_ago}: {users_by_apps.query('app == "feed" & date == @week_ago')['users'].values[0]}
Изменение: {((users_by_apps.query('app == "feed" & date == @last_day')['users'].values[0] / users_by_apps.query('app == "feed" & date == @week_ago')['users'].values[0] - 1) * 100).round(1)}%

✉️Мессенджер
DAU {last_day}: {users_by_apps.query('app == "message" & date == @last_day')['users'].values[0]}
DAU {week_ago}: {users_by_apps.query('app == "message" & date == @week_ago')['users'].values[0]}
Изменение: {((users_by_apps.query('app == "message" & date == @last_day')['users'].values[0] / users_by_apps.query('app == "message" & date == @week_ago')['users'].values[0] - 1) * 100).round(1)}%

📈Новые пользователи
C {week_ago_plus} по {last_day}: {new_users.groupby('reg_date', as_index=False).agg({'new_users': 'sum'})[24: 31]['new_users'].sum()}
C {two_week_ago} по {week_ago}: {new_users.groupby('reg_date', as_index=False).agg({'new_users': 'sum'})[17: 24]['new_users'].sum()}
Изменение: {((new_users.groupby('reg_date', as_index=False).agg({'new_users': 'sum'})[24: 31]['new_users'].sum() / new_users.groupby('reg_date', as_index=False).agg({'new_users': 'sum'})[17: 24]['new_users'].sum() - 1) * 100 ).round(2)}%

🪃Retention
1-го дня: {(retention.query('day == 1')['retention'].mean() * 100).round(1)}%
7-го дня: {(retention.query('day == 7')['retention'].mean() * 100).round(1)}%
14-го дня: {(retention.query('day == 14')['retention'].mean() * 100).round(1)}%'''

        bot.sendMessage(chat_id=chat_id, text=message)

    @task()
    def extract_actions():
        query = '''
        SELECT COUNT(user_id) as total_actions,
              action,
              toDate(time) AS date
        FROM simulator_20240320.feed_actions
        WHERE
            date >= today() - 31 AND
            date <= today() - 1
        GROUP BY action, date
        UNION ALL
        SELECT COUNT(user_id) as total_actions,
              'message' AS action,
              toDate(time) as date
        FROM simulator_20240320.message_actions
        WHERE
            date >= today() - 31 AND
            date <= today() - 1
        GROUP BY action, date
        '''
        actions = ph.read_clickhouse(query=query, connection=connection)
        return actions

    @task()
    def extract_quality():
        query = '''
        SELECT
            f.date,
            likes_per_user,
            views_per_user,
            messages_per_user
        FROM
            (SELECT
                toDate(time) AS date,
                countIf(user_id, action='like') / uniqExact(user_id) as likes_per_user,
                countIf(user_id, action='view') / uniqExact(user_id) as views_per_user
            FROM simulator_20240320.feed_actions
            WHERE
                date >= today() - 31 AND
                date <= today() - 1
            GROUP BY date) f
            LEFT JOIN
            (SELECT
                toDate(time) AS date,
                count(user_id) / uniqExact(user_id) as messages_per_user
            FROM simulator_20240320.message_actions
            WHERE
                date >= today() - 31 AND
                date <= today() - 1
            GROUP BY date) m
            USING(date)
        ORDER BY date
        '''
        quality = ph.read_clickhouse(query=query, connection=connection)
        return quality

    @task()
    def extract_posts():
        query = '''
        SELECT
            date,
            type,
            COUNT(post_id) AS posts
        FROM
            (SELECT
                date,
                post_id,
                post_date,
                if(post_date = date, 'new', 'old') AS type
            FROM
                (SELECT
                    DISTINCT
                    toDate(time) AS date,
                    post_id,
                    MIN(date) OVER(PARTITION BY post_id) AS post_date
                FROM simulator_20240320.feed_actions) t) t2
        WHERE
            date >= today() - 31 AND
            date <= today() - 1
        GROUP BY date, type
        ORDER BY date, type
        '''
        posts = ph.read_clickhouse(query=query, connection=connection)
        posts['date'] = posts['date'].apply(lambda x: x.strftime("%Y-%m-%d"))
        posts = posts.pivot(index='date', columns='type', values='posts').fillna(0)
        return posts
    
    @task()
    def send_actions_plots(actions, quality, posts):
        fig, axes = plt.subplots(3, 1, figsize=(7, 12))
        fig.suptitle('Активность\n', size='x-large', weight='bold')
        plot1, plot2, plot3 = range(3)
        
        # 1
        axes[plot1].title.set_text('Активность пользователей за последний месяц')
        axes[plot1] = sns.lineplot(x=actions['date'],
                                   y=actions['total_actions'],
                                   hue=actions['action'],
                                   palette='mako',
                                   ax=axes[plot1])
        
        axes[plot1].set(xlim=(actions['date'].min(),
                              actions['date'].max()),
                        xlabel=None,
                        ylim=(0, None),
                        ylabel=None)
        plt.setp(axes[plot1].get_xticklabels(), rotation=40)
        
        axes[plot1].xaxis.set_major_locator(mdates.DayLocator(interval=2))
        axes[plot1].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        axes[plot1].yaxis.set_major_formatter(ticker.EngFormatter())
        
        handles, labels = axes[plot1].get_legend_handles_labels()
        for i in range(len(labels)):
            if labels[i] == "like":
                labels[i] = 'Лайк'
            elif labels[i] == 'message':
                labels[i] = 'Сообщение'
            else:
                labels[i] = 'Просмотр'
        axes[plot1].legend(handles, labels, title = 'Действие:')
        
        # 2
        axes[plot2].title.set_text('Качество приложений')
        
        sns.lineplot(x=quality['date'],
                     y=quality['likes_per_user'],
                     color='#2e1e3b',
                     label='Лайков на пользователя ленты',
                     ax=axes[plot2])
        sns.lineplot(x=quality['date'],
                     y=quality['views_per_user'],
                     color='#37659e',
                     label='Просмотров на пользователя ленты',
                     ax=axes[plot2])
        sns.lineplot(x=quality['date'],
                     y=quality['messages_per_user'],
                     color='#40b7ad',
                     label='Отправленных сообщений на пользователя мессенджера',
                     ax=axes[plot2])
        
        axes[plot2].set(xlim=(quality['date'].min(),
                              quality['date'].max()),
                        xlabel=None,
                        ylim=(0, None),
                        ylabel=None)
        plt.setp(axes[plot2].get_xticklabels(), rotation=40)
        
        axes[plot2].xaxis.set_major_locator(mdates.DayLocator(interval=2))
        axes[plot2].xaxis.set_major_formatter(mdates.DateFormatter('%b %d'))
        axes[plot2].yaxis.set_major_formatter(ticker.EngFormatter())
        
        # 3
        axes[plot3].title.set_text('Посты в ленте новостей')
        
        posts.plot(kind='bar', stacked=True, color=['#2e1e3b', '#37659e'], ax=axes[plot3], rot=90)
        
        axes[plot3].set(xlabel=None, ylabel=None)
        axes[plot3].yaxis.set_major_formatter(ticker.EngFormatter())
        axes[plot3].xaxis.set_major_locator(ticker.MultipleLocator(2))
        
        handles, labels = axes[plot3].get_legend_handles_labels()
        for i in range(len(labels)):
            if labels[i] == "new":
                labels[i] = 'Новые'
            else:
                labels[i] = 'Старые'
        axes[plot3].legend(handles, labels, title='Посты:')
        
        fig.tight_layout()
        
        plot_object = io.BytesIO()
        fig.savefig(plot_object)
        plot_object.seek(0)
        plot_object.name = 'plot.png'
        plt.close()
        bot.sendPhoto(chat_id=chat_id, photo=plot_object)

    @task()
    def send_actions_info(actions, quality, posts):
        last_day = actions["date"].max().strftime("%d-%m-%Y")
        week_ago_plus = (actions["date"].max() - pd.to_timedelta(6, unit='d')).strftime("%d-%m-%Y")
        week_ago = (actions["date"].max() - pd.to_timedelta(7, unit='d')).strftime("%d-%m-%Y")
        two_week_ago = (actions["date"].max() - pd.to_timedelta(13, unit='d')).strftime("%d-%m-%Y")
        
        message = f'''📉Активность📈

❤️Лайков за {last_day}: {actions.query('action == "like" & date == @last_day')['total_actions'].values[0]} ({quality.query('date == @last_day')['likes_per_user'].values[0].round(1)} на пользователя)
❤️Лайков за {week_ago}: {actions.query('action == "like" & date == @week_ago')['total_actions'].values[0]} ({quality.query('date == @week_ago')['likes_per_user'].values[0].round(1)} на пользователя)
Изменение: {((actions.query('action == "like" & date == @last_day')['total_actions'].values[0] / actions.query('action == "like" & date == @week_ago')['total_actions'].values[0] - 1) * 100).round(1)}%

👀Просмотров за {last_day}: {actions.query('action == "view" & date == @last_day')['total_actions'].values[0]} ({quality.query('date == @last_day')['views_per_user'].values[0].round(1)} на пользователя)
👀Просмотров за {week_ago}: {actions.query('action == "view" & date == @week_ago')['total_actions'].values[0]} ({quality.query('date == @week_ago')['views_per_user'].values[0].round(1)} на пользователя)
Изменение: {((actions.query('action == "view" & date == @last_day')['total_actions'].values[0] / actions.query('action == "view" & date == @week_ago')['total_actions'].values[0] - 1) * 100).round(1)}%

✉️Сообщений за {last_day}: {actions.query('action == "message" & date == @last_day')['total_actions'].values[0]} ({quality.query('date == @last_day')['messages_per_user'].values[0].round(1)} на пользователя)
✉️Сообщений за {week_ago}: {actions.query('action == "message" & date == @week_ago')['total_actions'].values[0]} ({quality.query('date == @week_ago')['messages_per_user'].values[0].round(1)} на пользователя)
Изменение: {((actions.query('action == "message" & date == @last_day')['total_actions'].values[0] / actions.query('action == "message" & date == @week_ago')['total_actions'].values[0] - 1) * 100).round(1)}%'''

        bot.sendMessage(chat_id=chat_id, text=message)

    users_by_apps = extract_users_by_apps()
    new_users = extract_new_users()
    retention = extract_retention()
    new_gone_retained = extract_new_gone_retained()
    new_gone_retained_messenger = extract_new_gone_retained_messenger()
    send_users_plots(users_by_apps, new_users, new_gone_retained, new_gone_retained_messenger)
    send_users_info(users_by_apps, new_users, new_gone_retained, retention, new_gone_retained_messenger)

    actions = extract_actions()
    quality = extract_quality()
    posts = extract_posts()
    send_actions_plots(actions, quality, posts)
    send_actions_info(actions, quality, posts)

dag_n_anikin_tg_send_report = dag_n_anikin_tg_send_report()
