import json
import os
import time
import uuid

import altair as alt
import pandas as pd
import psycopg2
import streamlit as st
from kafka import KafkaProducer

KAFKA_BROKERS = os.getenv('KAFKA_BROKERS', 'kafka:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'transactions')
DB_URL = os.getenv('DATABASE_URL', 'postgresql://postgres:postgres@postgres:5432/fraud')


def send_to_kafka(df):
    """Отправляем каждую строку csv отдельным сообщением с уникальным id"""
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    )
    # NaN нельзя нормально положить в json, меняем на None
    df = df.astype(object).where(df.notna(), None)

    progress = st.progress(0)
    for i, (_, row) in enumerate(df.iterrows()):
        producer.send(KAFKA_TOPIC, value={
            'transaction_id': str(uuid.uuid4()),
            'data': row.to_dict(),
        })
        progress.progress((i + 1) / len(df))
        time.sleep(0.005)
    producer.flush()


def query_db(sql):
    """Выполняем запрос к Postgres и возвращаем DataFrame"""
    with psycopg2.connect(DB_URL) as conn:
        return pd.read_sql(sql, conn)


st.set_page_config(page_title='Fraud detection', page_icon='💳')
st.title('💳 Детекция фродовых транзакций')

# две вкладки: отправка данных и просмотр результатов
tab_send, tab_results = st.tabs(['📤 Отправка транзакций', '📊 Результаты'])

with tab_send:
    uploaded_file = st.file_uploader('Загрузите CSV файл с транзакциями (формат test.csv)', type=['csv'])
    if uploaded_file is not None:
        df = pd.read_csv(uploaded_file)
        st.write(f'Строк в файле: {len(df)}')
        st.dataframe(df.head())

        if st.button('Отправить в Kafka'):
            try:
                with st.spinner('Отправка...'):
                    send_to_kafka(df)
                st.success(f'Отправлено {len(df)} транзакций в топик {KAFKA_TOPIC}')
            except Exception as e:
                st.error(f'Ошибка отправки: {e}')

with tab_results:
    if st.button('Посмотреть результаты'):
        try:
            # 10 последних транзакций с флагом фрода
            frauds = query_db("""
                SELECT transaction_id, score, fraud_flag, created_at
                FROM scores
                WHERE fraud_flag = 1
                ORDER BY id DESC
                LIMIT 10
            """)
            # скоры последних 100 транзакций для гистограммы
            last_100 = query_db('SELECT score FROM scores ORDER BY id DESC LIMIT 100')

            st.subheader('10 последних фродовых транзакций')
            if frauds.empty:
                st.info('Фродовых транзакций пока нет')
            else:
                st.dataframe(frauds, use_container_width=True)

            st.subheader(f'Распределение скоров последних {len(last_100)} транзакций')
            if last_100.empty:
                st.info('В базе пока нет транзакций')
            else:
                chart = alt.Chart(last_100).mark_bar().encode(
                    x=alt.X('score:Q', bin=alt.Bin(maxbins=20), title='score'),
                    y=alt.Y('count():Q', title='количество'),
                )
                st.altair_chart(chart, use_container_width=True)
        except Exception as e:
            st.error(f'Ошибка запроса к базе: {e}')
