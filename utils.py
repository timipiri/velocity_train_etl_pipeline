import os
import sys

current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, ".."))

if project_root not in sys.path:
    sys.path.append(project_root)

from train_etl_pipeline import data_extraction_layer, data_transformation_layer, data_validation_layer, data_duplication_layer