# Load the api
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from api.loader import Load, LoadManager

# Load local library realsim
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from realsim.jobs import Job

GENERATOR = dict()
