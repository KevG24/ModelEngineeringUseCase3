# Import necessary libraries
from sklearn.datasets import load_iris
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, classification_report

class ModelTrainer:

    __data__ = []
    __targetData__ = []

    def __init__(self, data, targetData):
        self.__data__ = data
        self.__targetData__ = targetData

    def train(self):

        # Load the Iris dataset
        #iris = load_iris()
        #X = iris.data
        #y = iris.target
        X = self.__data__
        y = self.__targetData__
        # Split the data into training and testing sets
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)
        # Create and configure the Random Forest model
        rf_model = RandomForestClassifier(n_estimators=100, random_state=42)
        # Train the model
        rf_model.fit(X_train, y_train)
        # Make predictions on the test set
        y_pred = rf_model.predict(X_test)
        # Evaluate the model
        accuracy = accuracy_score(y_test, y_pred)
        print(f"Accuracy: {accuracy:.2f}")
        # Print classification report
        print("\nClassification Report:\n", classification_report(y_test, y_pred, target_names=iris.target_names))