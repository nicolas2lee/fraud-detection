from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, StreamTableEnvironment, DataTypes
from pyflink.table.descriptors import FileSystem, OldCsv, Schema
from pypmml import Model
import pandas as pd

class Stream:
    def read_data(self)-> pd.DataFrame:
        return pd.DataFrame()

    def run(self):
        exec_env = ExecutionEnvironment.get_execution_environment()
        exec_env.set_parallelism(1)
        t_config = TableConfig()
        t_env = StreamTableEnvironment.create(exec_env, t_config)

        t_env.connect(FileSystem().path('/tmp/input')) \
            .with_format(OldCsv()
                .field('word', DataTypes.STRING())) \
            .with_schema(Schema()
                .field('word', DataTypes.STRING())) \
            .create_temporary_table('mySource')

        t_env.connect(FileSystem().path('/tmp/output')) \
            .with_format(OldCsv()
                         .field_delimiter('\t')
                         .field('word', DataTypes.STRING())
                         .field('count', DataTypes.BIGINT())) \
            .with_schema(Schema()
                         .field('word', DataTypes.STRING())
                         .field('count', DataTypes.BIGINT())) \
            .create_temporary_table('mySink')
        model = Model.fromFile('./../batch_ml/model.pmml')

        t_env.from_path('mySource') \
            .group_by('word') \
            .select('word, count(1)') \
            .insert_into('mySink')

        t_env.execute("tutorial_job")


        self.read_data()
        result = model.predict({
            "Sepal_Length": 5.1,
            "Sepal_Width": 3.5,
            "Petal_Length": 1.4,
            "Petal_Width": 0.2
        })