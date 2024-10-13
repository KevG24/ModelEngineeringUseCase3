# Import necessary libraries
from sklearn.datasets import load_iris
from sklearn.ensemble import RandomForestClassifier, RandomForestRegressor
from sklearn.metrics import accuracy_score, classification_report
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

class RandomForestTrainer:

    __data__ = []
    __targetData__ = []

    def __init__(self, data, targetData):
        self.__data__ = data
        self.__targetData__ = targetData

    def train(self):

        X = self.__data__
        y = self.__targetData__

        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, shuffle=False)

        model = RandomForestRegressor(n_estimators=100)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mse = mean_squared_error(y_test, y_pred)
        logging.info(f'Mean Squared Error: {mse}')

        # Vorhersagen visualisieren
        #df_test = df.iloc[len(X_train):]
        #df_test['Vorhersage'] = y_pred
        #df_test[['Wert', 'Vorhersage']].plot()