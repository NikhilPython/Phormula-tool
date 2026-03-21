from config import Config
from dotenv import load_dotenv
from app.utils.token_utils import get_effective_user_id_from_token
from flask import Blueprint, Config, jsonify, request
from app.utils.web_scrapping_utils import analyze_business_website
import os
from app.models.user_models import UserObjective
from app import db
from datetime import date, datetime, timedelta

website_scrapper_bp = Blueprint("website_scrapper_bp", __name__)

load_dotenv()
 
