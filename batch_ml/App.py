# import spark_object_storage_demo_python.ibm_cos_helper as ibm_cos
import pandas as pd  # data processing, CSV file I/O (e.g. pd.read_csv)
#import pyspark.sql.functions as F
from sklearn.ensemble import RandomForestClassifier
from preprocessing.PreProcessor import PreProcessor

if __name__ == '__main__':

    FILE_PREFIX = "./resource/ieee-fraud-detection"

    train_identity = "train_identity.csv"
    train_transaction = "train_transaction.csv"
    test_identity = "test_identity.csv"
    test_transaction = "test_transaction.csv.csv"

    identity_train_df = pd.read_csv("{}/{}".format(FILE_PREFIX, train_identity))
    transaction_train_df_raw = pd.read_csv("{}/{}".format(FILE_PREFIX, train_transaction))

    identity_test_df = pd.read_csv("{}/{}".format(FILE_PREFIX, test_identity))
    X_final = pd.read_csv("{}/{}".format(FILE_PREFIX, test_transaction))
    print("===============finish file loading=================")
    Y_train = transaction_train_df_raw['isFraud']
    X_train = transaction_train_df_raw.drop('isFraud', axis=1)  # 506691

    preprocessor = PreProcessor(transaction_train_df_raw, identity_train_df)
    X_train_after_processing = preprocessor.preprocess()
    model = RandomForestClassifier(n_estimators=100, random_state=0)
    #my_pipeline = Pipeline(steps=[('preprocessor', preprocessor_pipeline), ('model', model)])

    # Preprocessing of training data, fit model
    # my_pipeline.fit(X_train, Y_train)
    #my_pipeline.fit(X_train, Y_train)

    from sklearn2pmml.pipeline import PMMLPipeline

    pipeline = PMMLPipeline([
        #("preprocessing", dataFrameMapper),
        ("classifier", model)
    ])
    pipeline.fit(X_train_after_processing, Y_train)

    from sklearn2pmml import sklearn2pmml

    sklearn2pmml(pipeline, "model.pmml", with_repr=True)

    # Preprocessing of validation data, get predictions
    #preds = my_pipeline.predict(X_test)