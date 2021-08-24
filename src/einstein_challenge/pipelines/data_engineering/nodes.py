# Copyright 2021 QuantumBlack Visual Analytics Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, AND
# NONINFRINGEMENT. IN NO EVENT WILL THE LICENSOR OR OTHER CONTRIBUTORS
# BE LIABLE FOR ANY CLAIM, DAMAGES, OR OTHER LIABILITY, WHETHER IN AN
# ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF, OR IN
# CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
# The QuantumBlack Visual Analytics Limited ("QuantumBlack") name and logo
# (either separately or in combination, "QuantumBlack Trademarks") are
# trademarks of QuantumBlack. The License does not grant you any right or
# license to the QuantumBlack Trademarks. You may not use the QuantumBlack
# Trademarks or any confusingly similar mark as a trademark for your product,
# or use the QuantumBlack Trademarks in any other manner that might cause
# confusion in the marketplace, including but not limited to in advertising,
# on websites, or on software.
#
# See the License for the specific language governing permissions and
# limitations under the License.
"""Example nodes to solve some common data engineering problems using PySpark,
such as:
* Extracting, transforming and selecting features
* Split data into training and testing datasets
"""

from posixpath import dirname
from typing import List

from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_date, regexp_replace, unix_timestamp, to_date, udf, when
from zipfile import ZipFile
from datetime import date
import shutil, os


def extract_from_zip_to_stage(zipfile_name):
    """"
    Extracts the files from the zip file, saving them in the correspondent
        table/month folder, in the stage layer
    """
    
    with ZipFile(zipfile_name, 'r') as zipObject:
        zipObject.extract('EINSTEIN_Exames_2.csv', path='data/stage/exames/agosto/')
    with ZipFile(zipfile_name, 'r') as zipObject:
        zipObject.extract('EINSTEIN_Pacientes_2.csv', path='data/stage/pacientes/agosto/')


    

def stage_to_raw(df_exames_stage, df_pacientes_stage):
    """
    Creates new parquet files based on the csv files, adding a new date column (DT_CARGA). 
        Files are saved correspondent table/month/ folder, in the raw layer.
    The original csv stage files copied to a history folder, to keep track of all the 
    files that were ingested in each particular date. 
    
    This was built specifically for this particular use case, where we only 
    have one zip file and know exactly what it's contents are. In a real life 
    scenario I would probably try to make this method more generic, using, for 
    instance, the file's name to check which month are we processing and 
    extract the files to that specific month's folder.
    """
    
    df_exames_raw = df_exames_stage.withColumn("DT_CARGA",current_date())
    df_pacientes_raw = df_pacientes_stage.withColumn("DT_CARGA",current_date())

    today = date.today().strftime("%Y-%m-%d")

    # Check if the folder exists or not
    for dir in ['exames','pacientes']:
        dirname = 'data/stage/'+dir+'/history/'+today+'/'
        if not os.path.isdir(dirname):
            # If not then make the new folder
            os.mkdir(dirname)
            
    shutil.copy('data/stage/exames/agosto/EINSTEIN_Exames_2.csv', 
        'data/stage/exames/history/'+today+'/')
    shutil.copy('data/stage/pacientes/agosto/EINSTEIN_Pacientes_2.csv', 
        'data/stage/pacientes/history/'+today+'/')

    return df_exames_raw, df_pacientes_raw


def convertToNull(df, columns_to_check):
    """
    Converts empty strings to Null on the selected columns of a dataframe
    """
    for i in df.columns:
        if i in columns_to_check:
            df = df.withColumn(i , when(col(i) == '', None ).otherwise(col(i)))
    return df


def lowerCase_and_one_space_only(input_str):
    """
    Converts a string to lowercase only and also removes any extra whitespace, leaving at most one whitespace between each word
    """
    
    final_string = input_str.lower()
    final_string = " ".join(final_string.split())

    return final_string


def quality_control_and_normalization_pacientes(df_pacientes):
    """
    Performs quality control and the normalization of the pacientes table.
    """

    df_pacientes = df_pacientes.withColumn('CD_MUNICIPIO', regexp_replace('CD_MUNICIPIO', 'MMMM', 'HIDEN'))
    df_pacientes = df_pacientes.withColumn('CD_CEPREDUZIDO', regexp_replace('CD_MUNICIPIO', 'CCCC', 'HIDEN'))


    columns_to_check = ["ID_PACIENTE","AA_NASCIMENTO","CD_PAIS","CD_MUNICIPIO"]
    df_pacientes = convertToNull(df_pacientes, columns_to_check)

    df_pacientes_hot = df_pacientes.na.drop(subset=columns_to_check)
    df_pacientes_rejected = df_pacientes.exceptAll(df_pacientes_hot)

    return df_pacientes_hot, df_pacientes_rejected


def quality_control_and_normalization_exames(df_exames):
    """
    Performs quality control and the normalization of the exames table.
    """

    df_exames = df_exames.withColumn('DT_COLETA',to_date(unix_timestamp(col('DT_COLETA'), 'dd/MM/yyyy').cast("timestamp")))

    lower_one_space_UDF = udf(lambda x:lowerCase_and_one_space_only(x))
    df_exames = df_exames.withColumn("DE_RESULTADO", lower_one_space_UDF(col("DE_RESULTADO")))

    columns_to_check = ["ID_PACIENTE","DT_COLETA","DE_EXAME","DE_RESULTADO"]
    df_exames = convertToNull(df_exames, columns_to_check)

    df_exames_hot = df_exames.na.drop(subset=columns_to_check) 
    df_exames_rejected = df_exames.exceptAll(df_exames_hot)

    return df_exames_hot, df_exames_rejected


def create_exames_por_paciente(df_exames, df_pacientes):
    """
    description
    """
    
    current_year = date.today().strftime("%Y")
    df_pacientes = df_pacientes.withColumn('VL_IDADE', (current_year-df_pacientes.AA_NASCIMENTO).cast('int'))

    exames_columns_to_drop = ['DT_CARGA']
    pacientes_columns_to_drop = ['AA_NASCIMENTO','DT_CARGA']
    df_exames = df_exames.drop(*exames_columns_to_drop)
    df_pacientes = df_pacientes.drop(*pacientes_columns_to_drop)

    df_exames_por_paciente = df_exames.join(df_pacientes, ['ID_PACIENTE'], how='left')
    df_exames_por_paciente_sp = df_exames_por_paciente.filter(df_exames_por_paciente.CD_UF == "SP")

    return df_exames_por_paciente, df_exames_por_paciente_sp

