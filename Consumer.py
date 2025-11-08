from kafka import KafkaConsumer
import json
import joblib
import numpy as np
from sqlalchemy import create_engine, inspect
from sqlalchemy.orm import sessionmaker

Consumer = KafkaConsumer(
    'MLTopic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=None,
    value_deserializer= lambda x: json.loads(x.decode('utf-8'))
)

Model = joblib.load('Model.pkl')

DBURL = "mysql+pymysql://root:@localhost:3333/workshop3"
Engine = create_engine(DBURL, echo=True)
Session = sessionmaker(bind=Engine)()
Inspector = inspect(Engine)
Conn = Engine.raw_connection()
Cursor = Conn.cursor()

TableName = 'kafkaconsumerdata'
ColumnNames = [Col['name'] for Col in Inspector.get_columns(TableName)]
Placeholder = ", ".join(["%s"] * len(ColumnNames))

print('Waiting for messages')
MessageCounter = 0

for Message in Consumer:

    MessageCounter += 1
    print('Messages received', MessageCounter)

    Features = list(Message.value.keys())
    Data = np.array(list(Message.value.values())[:-1]) #Exluding the 'y' colunn
    Y = list(Message.value.values())[-1]
    Prediction = Model.predict(Data.reshape(1,-1))[0]

    print('Received:', Data) 
    print('Happiness score for this country', Prediction)

    Cursor.execute(f"INSERT INTO {TableName} VALUES ({Placeholder})", [json.dumps(Features), Y, Prediction]) #Change the list type of features to avoid problems
    Conn.commit()
