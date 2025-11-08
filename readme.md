# Machine Learning predictions using Apache Kafka

## Context

The primary goal is to design and implement an ETL pipeline that ingests and processes a collection of CSV files containing historical World Happiness Report data from global surveys. Afterward, a regression model will be trained on the transformed data and later evaluated by applying it to new data streamed through a Kafka system and load the results to a database.


### About the dataset

The happiness scores and rankings use data from the Gallup World Poll. The scores are based on answers to the main life evaluation question asked in the poll. This question, known as the Cantril ladder, asks respondents to think of a ladder with the best possible life for them being a 10 and the worst possible life being a 0 and to rate their own current lives on that scale. The columns following the happiness score estimate the extent to which each of six factors – economic production, social support, life expectancy, freedom, absence of corruption, and generosity – contribute to making life evaluations higher in each country than they are in Dystopia, a hypothetical country that has values equal to the world’s lowest national averages for each of the six factors.

Full information: https://www.kaggle.com/datasets/unsdsn/world-happiness


### Tech stack

- Python (Numpy, Pandas, Matplotlib, Sklearn, Statsmodels)
- Apache Kafka
- MySQL


### EDA

![Link](https://imgur.com/Jh16mxe.jpeg)
![Link](https://imgur.com/PjYm0FC.jpeg)
![Link](https://imgur.com/LXjk3x2.jpeg)

### Transformations

- Merge all the datasets together without including the year every record was taken on.
- Imputed the 'Dystopia' variable using the MICE method (Multiple Imputation by Chained Ecuations).
- Selected the optimal features to use in the model training using a forward selection method.

### Results 

![Link](https://imgur.com/hPG6T4S.jpeg)
![Link](https://imgur.com/LZN5MjL.jpeg)
![Link](https://imgur.com/bCQL8PM.jpeg)
![Link](https://imgur.com/egdmBOZ.jpeg)


