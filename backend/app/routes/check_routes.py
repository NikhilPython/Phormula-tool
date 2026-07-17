from flask import Blueprint, request, jsonify , send_file
from sqlalchemy import create_engine, MetaData, text, inspect
from sqlalchemy.orm import sessionmaker, scoped_session
from zoneinfo import ZoneInfo
from app.utils.token_utils import get_effective_user_id_from_token
import jwt
import os
import base64
import re
from datetime import datetime 
import pandas as pd
from config import Config
SECRET_KEY = Config.SECRET_KEY
from app.models.user_models import User , CountryProfile
from app import db
from dotenv import load_dotenv
from datetime import datetime
from io import BytesIO


load_dotenv()
db_url = os.getenv('DATABASE_URL')
db_url1 = os.getenv('DATABASE_ADMIN_URL')



check_bp = Blueprint('check_bp', __name__)

