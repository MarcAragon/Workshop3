from kafka import KafkaProducer
import pandas as pd
import json
import random
import time

import numpy as np
np.random.seed(1738)

from statsmodels.imputation.mice import MICEData
from sklearn.model_selection import train_test_split

Producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

Df2015 = pd.read_csv('SourceData/2015.csv')
Df2016 = pd.read_csv('SourceData/2016.csv')
Df2017 = pd.read_csv('SourceData/2017.csv')
Df2018 = pd.read_csv('SourceData/2018.csv')
Df2019 = pd.read_csv('SourceData/2019.csv')

CountriesToRegion = dict(zip(Df2015['Country'], Df2015['Region']))

#Dropping useless features
Df2015.drop(['Country', 'Standard Error', 'Happiness Rank'], axis=1, inplace=True)
Df2016.drop(['Country', 'Lower Confidence Interval', 'Upper Confidence Interval', 'Happiness Rank'], axis=1, inplace=True)
Df2017.drop(['Whisker.high', 'Whisker.low', 'Happiness.Rank'], axis=1, inplace=True)
Df2018.drop('Overall rank', axis=1, inplace=True)
Df2019.drop('Overall rank', axis=1, inplace=True)

#Normalizing columns to merge
Df2017 = Df2017[['Country', 'Happiness.Score', 'Economy..GDP.per.Capita.', 'Family', 'Health..Life.Expectancy.', 'Freedom', 'Trust..Government.Corruption.', 'Generosity','Dystopia.Residual']]
Df2017.columns = Df2016.columns
Df2018 = Df2018[['Country or region', 'Score', 'GDP per capita', 'Social support', 'Healthy life expectancy', 'Freedom to make life choices', 'Perceptions of corruption', 'Generosity']]
Df2018.columns = ['Region', 'Happiness Score', 'Economy (GDP per Capita)', 'Family', 'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)', 'Generosity']
Df2019 = Df2019[['Country or region', 'Score', 'GDP per capita', 'Social support', 'Healthy life expectancy', 'Freedom to make life choices', 'Perceptions of corruption', 'Generosity']]
Df2019.columns = ['Region', 'Happiness Score', 'Economy (GDP per Capita)', 'Family', 'Health (Life Expectancy)', 'Freedom', 'Trust (Government Corruption)', 'Generosity']

Df2017['Region'] = Df2017['Region'].map(CountriesToRegion).values
Df2018['Region'] = Df2018['Region'].map(CountriesToRegion).values
Df2019['Region'] = Df2019['Region'].map(CountriesToRegion).values

Df = pd.concat([Df2015, Df2016, Df2017, Df2018, Df2019], axis=0)
Df.columns = [Column.replace(' ', '_').replace('(', '').replace(')', '') for Column in Df.columns]

Predictors = Df.drop(columns=['Happiness_Score', 'Region'])
MiceData = MICEData(Predictors)

for i in range(20): 
    MiceData.update_all()
      
ImputedDf = MiceData.data.copy()
ImputedDf['Region'] = Df['Region'].values
ImputedDf['Happiness_Score'] = Df['Happiness_Score'].values

DummyDf = pd.get_dummies(ImputedDf, columns=['Region'], drop_first=True)
DummyDf.drop('Health_Life_Expectancy', axis=1, inplace=True)

_, X_Test, _, Y_Test = train_test_split(DummyDf.drop('Happiness_Score', axis=1).astype(float), DummyDf['Happiness_Score'], train_size=.7)
X_Test = X_Test[['Economy_GDP_per_Capita','Family','Freedom','Generosity','Dystopia_Residual','Region_Latin America and Caribbean','Region_Western Europe']] #Selected features
StreamDf = pd.concat([X_Test, Y_Test], axis=1)


#Streaming the data to the consumer
for _, Row in StreamDf.iterrows():
    Data = Row.to_dict()
    Producer.send('MLTopic', value=Data)
    print('Data send', Data)
    time.sleep(random.uniform(.1, .5))

Producer.flush()
print('Mensajes enviados')