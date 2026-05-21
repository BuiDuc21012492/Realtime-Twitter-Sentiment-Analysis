import dash
from dash import dcc, html, Output, Input
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from kafka import KafkaConsumer
import json
import threading
from collections import deque
import time
import logging
from sklearn.metrics import confusion_matrix

# --- Cấu hình log để dễ dàng theo dõi ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Cấu hình Kafka ---
KAFKA_SERVER = 'kafka:29092'
KAFKA_TOPIC = 'sentiment_topic'

# --- Lưu trữ dữ liệu ---
MAX_DATAPOINTS = 15000  # Đủ lớn để chứa hết dữ liệu từ file CSV
data_points = deque(maxlen=MAX_DATAPOINTS)
app_lock = threading.Lock() # Lock để tránh xung đột khi nhiều thread cùng truy cập data_points

# --- Thread chạy ngầm để đọc Kafka ---
def consume_messages():
    while True:
        consumer = None
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_SERVER,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='earliest',
                group_id='dashboard-final-consumer-group', # Dùng group_id mới để đảm bảo đọc lại từ đầu
                api_version=(2, 7)
            )
            logging.info(f"Dashboard consumer connected to Kafka topic '{KAFKA_TOPIC}'.")
            for message in consumer:
                with app_lock:
                    data_points.append(message.value)
                # Logging sau mỗi 100 message để đỡ nhiễu
                if len(data_points) % 100 == 0:
                    logging.info(f"Received {len(data_points)} messages so far...")
        except Exception as e:
            logging.error(f"Dashboard Kafka connection error: {e}. Retrying in 5 seconds...")
            if consumer:
                consumer.close()
            time.sleep(5)

# --- Khởi tạo Dash App ---
app = dash.Dash(__name__, external_stylesheets=['https://codepen.io/chriddyp/pen/bWLwgP.css'])
app.title = "Airline Sentiment Analysis Dashboard"

# --- Bố cục Giao diện Mới ---
app.layout = html.Div(style={'backgroundColor': '#F9F9F9'}, children=[
    # Header
    html.Div(
        style={'backgroundColor': '#1E88E5', 'padding': '15px', 'color': 'white', 'textAlign': 'center', 'boxShadow': '0 2px 5px 0 rgba(0,0,0,0.16)'},
        children=[html.H1("Airline Tweet Sentiment Analysis Dashboard", style={'margin': '0'})]
    ),
    
    # Component tự động trigger cập nhật
    dcc.Interval(id='interval-component', interval=5*1000, n_intervals=0),

    # Phần nội dung chính
    html.Div(style={'padding': '20px'}, children=[
        # Hàng 1: Các biểu đồ tổng quan
        html.Div(className="row", style={'display': 'flex', 'marginBottom': '20px'}, children=[
            dcc.Graph(id='sentiment-pie-chart', style={'flex': 1, 'boxShadow': '0 2px 5px 0 rgba(0,0,0,0.1)'}),
            dcc.Graph(id='sentiment-bar-chart', style={'flex': 1.5, 'marginLeft': '20px', 'boxShadow': '0 2px 5px 0 rgba(0,0,0,0.1)'}),
        ]),
        # Hàng 2: Biểu đồ đánh giá mô hình
        html.Div(className="row", children=[
            dcc.Graph(id='confusion-matrix-chart', style={'boxShadow': '0 2px 5px 0 rgba(0,0,0,0.1)'})
        ])
    ])
])

# --- Callback để cập nhật tất cả các biểu đồ ---
@app.callback(
    [Output('sentiment-pie-chart', 'figure'),
     Output('sentiment-bar-chart', 'figure'),
     Output('confusion-matrix-chart', 'figure')],
    [Input('interval-component', 'n_intervals')]
)
def update_all_graphs(n):
    # Lấy dữ liệu một cách an toàn
    with app_lock:
        if not data_points:
            # Trả về các biểu đồ trống với thông báo chờ
            empty_figure = go.Figure().update_layout(
                xaxis={"visible": False}, yaxis={"visible": False},
                annotations=[{"text": "Waiting for data from Kafka...", "showarrow": False, "font": {"size": 20}}]
            )
            return empty_figure, empty_figure, empty_figure
        
        df = pd.DataFrame(list(data_points))

    # --- 1. Tạo biểu đồ tròn (Pie Chart) ---
    pie_counts = df['predicted_sentiment'].value_counts()
    pie_fig = px.pie(
        values=pie_counts.values, names=pie_counts.index,
        title=f'Sentiment Distribution (Total: {len(df)})',
        color_discrete_map={'positive': '#27AE60', 'negative': '#C0392B', 'neutral': '#BDC3C7'},
        hole=.4
    )
    pie_fig.update_layout(title_x=0.5, margin=dict(t=50, b=20, l=20, r=20))

    # --- 2. Tạo biểu đồ cột (Bar Chart) ---
    bar_counts = df['predicted_sentiment'].value_counts().reset_index()
    bar_counts.columns = ['sentiment', 'count']
    bar_fig = px.bar(
        bar_counts.sort_values('count'),
        x='count', y='sentiment', orientation='h',
        color='sentiment',
        title='Sentiment Counts',
        labels={'count': 'Number of Tweets', 'sentiment': 'Sentiment'},
        color_discrete_map={'positive': '#27AE60', 'negative': '#C0392B', 'neutral': '#BDC3C7'}
    )
    bar_fig.update_layout(showlegend=False, title_x=0.5)

    # --- 3. Tạo ma trận nhầm lẫn (Confusion Matrix) ---
    labels = ['positive', 'neutral', 'negative']
    y_true = df['ground_truth_sentiment']
    y_pred = df['predicted_sentiment']
    
    cm = confusion_matrix(y_true, y_pred, labels=labels)
    
    # Tạo heatmap
    cm_fig = go.Figure(data=go.Heatmap(
        z=cm,
        x=labels,
        y=labels,
        hoverongaps=False,
        colorscale='Blues',
        text=cm,
        texttemplate="%{text}"
    ))
    cm_fig.update_layout(
        title_text='Confusion Matrix: Model Prediction vs. Ground Truth',
        xaxis_title="Predicted Sentiment (Dự đoán của mô hình)",
        yaxis_title="True Sentiment (Nhãn gốc)",
        title_x=0.5
    )

    return pie_fig, bar_fig, cm_fig


if __name__ == '__main__':
    # Chạy consumer trong một thread riêng để không block ứng dụng web
    consumer_thread = threading.Thread(target=consume_messages)
    consumer_thread.daemon = True
    consumer_thread.start()
    
    app.run(host='0.0.0.0', port=8050)