import category_encoders as ce
from pandas import DataFrame
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline


class PreProcessor:
    identity_train_df = DataFrame()
    transaction_train_df_raw = DataFrame()

    def __init__(self, transaction_train_df_raw, identity_train_df):
        self.transaction_train_df_raw = transaction_train_df_raw
        self.identity_train_df = identity_train_df

    def preprocess(self) -> DataFrame:
        X_train = self.transaction_train_df_raw.drop('isFraud', axis=1)  # 506691

        # num_test = 0.20
        # X_all = transaction_train_df_raw.drop('isFraud', axis=1)
        # Y_all = transaction_train_df_raw['isFraud']
        # X_train, X_test, Y_train, Y_test = train_test_split(X_all, Y_all, test_size=num_test)

        # Preprocessing for numerical data
        numerical_transformer = SimpleImputer(strategy='mean')
        # Preprocessing for categorical data
        # to implment addr2, divide into 2 categories, then one hot enconding
        onehot_categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('onehot', ce.OneHotEncoder())
        ])

        binary_categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='most_frequent')),
            ('binary', ce.BinaryEncoder(drop_invariant=False))
        ])

        ordinary_categorical_transformer = Pipeline(steps=[
            ('imputer', SimpleImputer(strategy='mean')),
            ('ordinary', ce.OrdinalEncoder())
        ])

        # Bundle preprocessing for numerical and categorical data
        preprocessor_pipeline = ColumnTransformer(
            transformers=[
                ('num', numerical_transformer,
                 ['TransactionDT', 'TransactionAmt', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12',
                  'C13', 'C14', 'D1', 'D4', 'D10', 'D15']),
                ('onehot_cat', onehot_categorical_transformer, ['ProductCD', 'card4', 'M6']),
                #         ('onehot_cat', onehot_categorical_transformer, ['ProductCD', 'card4', 'card6', 'M6']),
                #        binary encoder did not work, should re implement
                #        ('binary_cat', binary_categorical_transformer, ['card3', 'card5', 'addr1', 'addr2']),
                ('binary_cat', binary_categorical_transformer, ['P_emaildomain']),
                ('ordinary_cat', ordinary_categorical_transformer, ['card1', 'card2'])

            ])

        # dataFrameMapper = DataFrameMapper([
        #         (['TransactionDT', 'TransactionAmt', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'C9', 'C10', 'C11', 'C12', 'C13', 'C14', 'D1', 'D4', 'D10', 'D15'], numerical_transformer),
        #         (['ProductCD', 'card4', 'M6'], onehot_categorical_transformer),
        #         #         ('onehot_cat', onehot_categorical_transformer, ['ProductCD', 'card4', 'card6', 'M6']),
        #         #        binary encoder did not work, should re implement
        #         #        ('binary_cat', binary_categorical_transformer, ['card3', 'card5', 'addr1', 'addr2']),
        #         (['P_emaildomain'], binary_categorical_transformer ),
        #         (['card1', 'card2'], ordinary_categorical_transformer)
        #     ]
        # )
        return preprocessor_pipeline.fit_transform(X_train)