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

"""Example data engineering pipeline with PySpark.
"""

from .nodes import *
from kedro.pipeline import Pipeline, node

def create_pipeline(**kwargs):
    
    #before creating and running the actual pipeline, we extract the contents of the zip file
    extract_from_zip_to_stage("data/stage/EINSTEINAgosto.zip")
    
    return Pipeline(
        [
            node(
                stage_to_raw,
                inputs = ["stage_exames", "stage_pacientes"],
                outputs = ["raw_exames", "raw_pacientes"]
            ),
            node(
                quality_control_and_normalization_pacientes,
                inputs = ["raw_pacientes"],
                outputs = ["curated_pacientes_hot", "curated_pacientes_rejected"]
            ),
            node(
                quality_control_and_normalization_exames,
                inputs = ["raw_exames"],
                outputs = ["curated_exames_hot", "curated_exames_rejected"]
            ),
            node(
                create_exames_por_paciente,
                inputs = ["curated_exames_hot", "curated_pacientes_hot"],
                outputs = ["exames_por_paciente", "exames_por_paciente_sp"]
            ),
            
        ]
    )
