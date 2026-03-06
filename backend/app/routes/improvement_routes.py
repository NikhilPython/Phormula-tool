# from flask import Blueprint, request, jsonify
# import jwt
# import os
# from app import db
# from flask import current_app
# from datetime import datetime, timedelta
# import openai
# from sqlalchemy import create_engine, inspect, text
# from dotenv import load_dotenv
# from config import Config
# from app.utils.token_utils import get_effective_user_id_from_token
# import re
# import calendar
# from calendar import month_name as calendar_month_name
# import json
# from sqlalchemy import text
# from dateutil.relativedelta import relativedelta
# import pandas as pd
# from app.models.user_models import improvment
# from collections import defaultdict

# # Load environment variables
# load_dotenv()
# SECRET_KEY = Config.SECRET_KEY
# OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
# db_url = os.getenv('DATABASE_URL')

# # Initialize OpenAI client
# openai.api_key = OPENAI_API_KEY

# # Initialize database engine
# engine = create_engine(db_url)

# improvement_bp = Blueprint('improvement_bp', __name__)

# # Currency mapping for different countries
# CURRENCY_CONFIG = {
#     'uk': {'symbol': '£', 'code': 'GBP', 'name': 'British Pound'},
#     'us': {'symbol': '$', 'code': 'USD', 'name': 'US Dollar'},
#     'ca': {'symbol': 'C$', 'code': 'CAD', 'name': 'Canadian Dollar'},
#     'in': {'symbol': '₹', 'code': 'INR', 'name': 'Indian Rupee'},
# }

# def get_currency_info(country_code):
#     """Get currency information for the given country"""
#     country_lower = country_code.lower()
#     return CURRENCY_CONFIG.get(country_lower, {'symbol': '$', 'code': 'USD', 'name': 'US Dollar'})

# def debug_table_search(user_id, country, all_tables):
#     """Debug function to help identify why tables aren't being found"""
#     # Updated for actual table pattern: skuwisemonthly_userid_country_monthyear
#     pattern = f"skuwisemonthly_{user_id}_{country}_"
    
#     # Show all tables that start with 'skuwisemonthly'
#     sku_tables = [t for t in all_tables if t.startswith('skuwisemonthly')]
    
#     # Show tables that match user_id
#     user_tables = [t for t in all_tables if f"_{user_id}_" in t]
    
#     # Show tables that match country  
#     country_tables = [t for t in all_tables if f"_{country}_" in t.lower()]
    
#     # Show tables that match the full pattern
#     pattern_tables = [t for t in all_tables if t.startswith(pattern)]
    
#     return {
#         'pattern': pattern,
#         'all_sku_tables': sku_tables,
#         'user_tables': user_tables,
#         'country_tables': country_tables,
#         'pattern_tables': pattern_tables
#     }

# #################################################################################################################

# def collect_3_month_history(valid_tables, engine):
#     """Return product_name → 3-month performance list"""
#     recent_tables = valid_tables[:4]
#     history = defaultdict(list)

#     for table_name, table_date in recent_tables:
#         try:
#             df = pd.read_sql_query(text(f"""
#                 SELECT product_name, unit_growth, asp_growth, sales_growth, profit_growth
#                 FROM "{table_name}"
#             """), engine)
#             month_label = table_date.strftime('%b')
#             for _, row in df.iterrows():
#                 history[row['product_name']].append({
#                     "month": month_label,
#                     "unit_growth": row['unit_growth'],
#                     "asp_growth": row['asp_growth'],
#                     "sales_growth": row['sales_growth'],
#                     "profit_growth": row['profit_growth']
#                 })
#         except Exception as e:
#             print(f"⚠️ Failed to collect history from {table_name}: {e}")
#     return history


# def generate_trend_footnote(product_name, performance_history):
#     if len(performance_history) < 2:
#         return ""

#     # Ensure we always show enough history for a trend, up to 3 months for conciseness
#     # but use the full history for the LLM if it's longer than 3, just for context
#     perf_display = "\n".join(
#         f"{entry['month']}: Units {entry['unit_growth']}, ASP {entry['asp_growth']}, "
#         f"Sales {entry['sales_growth']}, Profit {entry['profit_growth']}"
#         for entry in performance_history
#     )

#     prompt = (
#     f"Product: {product_name}\n"
#     f"Monthly Performance Data (oldest→newest):\n{perf_display}\n\n"
#     "Write ONE concise sentence (max 25 words) describing the overall performance trend only, using explicit month-to-month % changes and transitions (e.g., “Apr to May”). "
#     "Purely descriptive—no recommendations or actions, no invented data."
# )

#     try:
#         response = openai.ChatCompletion.create(
#             model="gpt-4",
#             messages=[
#                 {"role": "system", "content": "You are a concise D2C data analyst."},
#                 {"role": "user", "content": prompt}
#             ],
#             max_tokens=60, # Keep max_tokens a bit higher to allow for a slightly longer insightful sentence
#             temperature=0.5 # Lower temperature for more focused, less creative summaries
#         )
#         return response['choices'][0]['message']['content'].strip()
#     except Exception as e:
#         print(f"⚠️ AI footnote error for {product_name}: {e}")
#         return ""


# #######################################################################################################################

# def get_top_80_percent_skus(sku_data, exclude_skus=None):
#     if not sku_data:
#         return []

#     exclude_skus = set(exclude_skus or [])

#     # Filter out 'TOTAL' row and excluded SKUs
#     filtered_skus = [
#         sku for sku in sku_data 
#         if sku['product_name'].strip().lower() != 'total' 
#         and sku['sku'] not in exclude_skus
#     ]

#     # Sort descending by sales_mix
#     sorted_skus = sorted(filtered_skus, key=lambda x: x['sales_mix'], reverse=True)

#     cumulative = 0.0
#     top_skus = []

#     for sku in sorted_skus:
#         cumulative += sku['sales_mix']
#         top_skus.append(sku)
#         if cumulative >= 80.0:
#             break

#     return top_skus


# @improvement_bp.route('/improvement_tab_suggestion', methods=['POST'])
# def improvement_tab_suggestion():
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     if not request.is_json:
#         return jsonify({'error': 'Request must be in JSON format'}), 400

#     json_data = request.get_json()
#     country = json_data.get('country', '').lower().strip()
#     if not country:
#         return jsonify({'error': 'Country is required'}), 400

#     try:
#         currency_info = get_currency_info(country)

#         inspector = inspect(engine)
#         all_tables = inspector.get_table_names()
#         base_pattern = f"skuwisemonthly_{user_id}_{country}_"
#         matched_tables = [t for t in all_tables if t.startswith(base_pattern)]

#         valid_tables = []
#         for t in matched_tables:
#             match = re.search(r'_([a-zA-Z]+)(\d{4})$', t)
#             if match:
#                 month_str, year_str = match.group(1).capitalize(), match.group(2)
#                 try:
#                     month_num = list(calendar.month_name).index(month_str)
#                     date_obj = datetime(int(year_str), month_num, 1)
#                     valid_tables.append((t, date_obj))
#                 except ValueError:
#                     continue

#         valid_tables.sort(key=lambda x: x[1], reverse=True)
#         if not valid_tables:
#             return jsonify({
#                 'data_status': 'no_data_found',
#                 'currency': currency_info,
#                 'message': f'No valid tables found for user {user_id} in country {country}.'
#             }), 200

#         latest_table, latest_date = valid_tables[0]
#         current_month = latest_date.strftime("%B")
#         current_year = latest_date.strftime("%Y")
#         previous_date = latest_date - timedelta(days=1)
#         prev_month = previous_date.strftime("%B")
#         prev_year = previous_date.strftime("%Y")

#         query = f"""
#             SELECT 
#                 product_name, sku,
#                 unit_growth, asp_growth, sales_growth, profit_growth,
#                 unit_wise_profitability_growth, sales_mix,
#                 unit_increase, asp_percentag, sales_percentage,
#                 unit_wise_profitability_percentage, profit_change,
#                 sales_mix_percentage, unit_wise_amazon_fee_percentage,
#                 profit_mix_percentage, sales_mix_growth,
#                 amazon_fee_growth, profit_mix_growth
#             FROM "{latest_table}"
#         """
#         df_all = pd.read_sql_query(text(query), engine)
#         df_all = df_all.dropna(subset=['product_name', 'sales_mix'])

#         sku_data = df_all.to_dict(orient='records')
#             # ✅ Exclude new/resurrected SKUs
#         sku_changes = analyze_sku_patterns(user_id, country)
#         excluded_skus = set(sku_changes.get("new_skus", []) + sku_changes.get("resurrected_skus", []))

#     # ✅ Apply exclusion in top 80% logic
#         top_skus = get_top_80_percent_skus(sku_data, exclude_skus=excluded_skus)
#         df = pd.DataFrame(top_skus)
# # 3-month performance data for all SKUs
#         history_map = collect_3_month_history(valid_tables, engine)


#         def format_latest_metrics(row):
#             metrics = {
#                 "Unit Growth": row.get("unit_increase"),
#                 "ASP": row.get("asp_percentag"),
#                 "Sales": row.get("sales_percentage"),
#                 "Profit": row.get("profit_change"),
#                 "Unit Profitability": row.get("unit_wise_profitability_percentage"),
#             }
#             result = []
#             for key, val in metrics.items():
#                 try:
#                     formatted_val = f"{float(val):.2f}%" if val is not None else "Data not available"
#                 except Exception:
#                     formatted_val = "Data not available"
#                 result.append(f"- {key}: {formatted_val}")
#             return "\n".join(result)

#         SYSTEM_PROMPT = """Let's refine that prompt to truly embody a Senior D2C Data Analyst, someone who not only analyzes data but also understands the strategic implications for a direct-to-consumer business.
#         Here's the enhanced prompt, focusing on depth, D2C specific metrics, and actionable recommendations with business context:

# You are a highly concise, deeply insightful, and unequivocally action-oriented Senior D2C Data Analyst. Your core mandate is to rapidly dissect complex historical product performance data for a Direct-to-Consumer (D2C) business. You must translate raw metrics into critical observations and precise, implementable strategic recommendations that directly impact D2C growth, profitability, customer acquisition cost (CAC), customer lifetime value (CLTV), and overall brand health.

# Your analytical approach will strictly adhere to these principles:

# D2C Business Acumen: Every observation and recommendation must be framed within the context of a D2C business model. Consider the direct relationship with the customer, implications for inventory management, supply chain efficiency, digital marketing efficacy, conversion funnel optimization, and customer retention strategies.

# Temporal Precision & Trend Identification: Always articulate performance changes using exact month-to-month transitions (e.g., 'Jan to Feb', 'Feb to Mar', 'Mar to Apr'). Never use vague terms like 'recently'. Crucially, compare at least two sequential periods to identify clear trends, acceleration, or deceleration, providing the 'story' behind the numbers.

# Data-Driven Specificity: Provide concrete percentages and absolute values (where applicable) for every observation. Avoid qualitative judgments without quantitative backing. Your analysis is precise and data-backed.

# Actionability & Strategic Impact: Every single observation must lead directly to a concrete, data-justified recommendation. These recommendations should propose specific D2C levers to pull (e.g., re-segmenting ad audiences, adjusting fulfillment strategies, optimizing website checkout flow, launching retention campaigns), always explaining the anticipated business impact.
#                     """


#         # Updated AI logic to return bullet points
#         def analyze_row(row):
#             with current_app.app_context():  # ✅ Ensures Flask context is available in thread
#                 try:
#                     feedback = improvment.query.filter_by(
#                         user_id=user_id,
#                         country=country,
#                         product=row['product_name']
#                     ).order_by(improvment.id.desc()).first()

#                     feedback_summary = ""
#                     if feedback:
#                         feedback_type = "positive" if feedback.feedback_type == "like" else "critical"
#                         feedback_summary = f"Past user feedback ({feedback_type}): {feedback.feedback_text}\n\n"

#                     product_history = history_map.get(row['product_name'], [])
#                     usable_deltas = (
#                         product_history[-3:] if len(product_history) >= 3
#                         else product_history[-2:] if len(product_history) == 2
#                         else product_history[-1:]
#                     )

#                     month_transitions = {
#                         "January": "Dec → Jan", "February": "Jan → Feb", "March": "Feb → Mar",
#                         "April": "Mar → Apr", "May": "Apr → May", "June": "May → Jun",
#                         "July": "Jun → Jul", "August": "Jul → Aug", "September": "Aug → Sep",
#                         "October": "Sep → Oct", "November": "Oct → Nov", "December": "Nov → Dec"
#                     }

#                     history_summary = "\n".join([
#                         f"- {month_transitions.get(entry['month'], entry['month'])}: "
#                         f"Units {entry['unit_growth']}%, ASP {entry['asp_growth']}%, "
#                         f"Sales {entry['sales_growth']}%, Profit {entry['profit_growth']}%"
#                         for entry in usable_deltas
#                     ]) if usable_deltas else "Not enough historical data."

#                     latest_metrics_formatted = format_latest_metrics(row)

#                     prompt = f"""
#         {feedback_summary}
# Product: {row['product_name']}

# 📉 Historical Performance Deltas:
# {history_summary}

# 📈 Latest Month Detailed Metrics:
# {latest_metrics_formatted}

# ✍️ Write the analysis in the following exact format:

# Details for {row['product_name']}

# Observations
# 1. Write 2-3 numbered bullets summarizing overall performance. Each bullet must weave multiple metrics (Unit Growth, Sales, ASP, Profit) together, cite % changes, and use month transitions (e.g., “Apr to May”, “May to Jun”). Highlight the biggest swings; avoid per-metric repetition here.

# Improvements
# 1. Write 2-3 numbered bullets with strategy-level actions. Do not list every metric again. Tie each recommendation to the patterns and months above (e.g., “Reverse the Jun unit drop by…”).

# Unit Growth
# • Observation: Describe the multi-month trend with explicit month transitions and % shifts. Call out sharp inflections.
# • Improvement: One concrete lever to fix declines or scale gains (e.g., channel mix, promo cadence). Reference months if relevant.

# ASP
# • Observation: Precisely state how ASP moved month to month with % values—no vague terms like “slight”.
# • Improvement: Actionable pricing tactic (bundles, premium SKU, controlled discount window, A/B price test).

# Sales
# • Observation: Explain Sales changes and link them to Unit Growth and/or ASP. Mention every recent month-to-month shift.
# • Improvement: Focused actions to lift Sales (campaign timing, replicate high-growth periods, channel expansion).

# Profit
# • Observation: Note whether Profit followed Sales or diverged. Use % and months (e.g., “Profit +12% in May despite flat Sales”).
# • Improvement: Margin-focused tactic (lower COGS, bundle for ASP lift, replicate profitable price windows).

# Unit Profitability
# • Observation: State how per-unit profit moved across the same range—rising, falling, or stable (quantify even small moves).
# • Improvement: Steps to maintain/boost it (supplier renegotiation, drop low-margin variants, packaging/fulfillment efficiency).

# Use this exact structure and formatting but do not give prefix observation and improvements in the final output. Tone must reflect a senior business analyst writing to a leadership team. No extra summaries or generic advice — base all insights on provided data only, never invent missing numbers.

#         """

#                     response = openai.ChatCompletion.create(
#                         model="gpt-4",
#                         messages=[
#                             {"role": "system", "content": SYSTEM_PROMPT},
#                             {"role": "user", "content": prompt}
#                         ],
#                         max_tokens=600,
#                         temperature=0.3,
#                     )

                    

#                     content = response['choices'][0]['message']['content']

#                     section_headers = [
#                         "Observations", "Improvements",
#                         "Unit Growth", "ASP", "Sales", "Profit", "Unit Profitability"
#                     ]
#                     parts = re.split(r'^\s*(' + '|'.join(re.escape(h) for h in section_headers) + r')\s*$',
#                                     content.strip(), flags=re.MULTILINE)

#                     section_map = {}
#                     i = 1
#                     while i < len(parts):
#                         label = parts[i].strip()
#                         body = parts[i + 1].strip() if i + 1 < len(parts) else ''
#                         section_map[label] = body
#                         i += 2

#                     def extract_bullets(text):
#                         return "\n".join([l.strip("•- \t") for l in text.splitlines() if l.strip()])

#                     def extract_metric_bullets(text):
#                         bullets = [line.strip("•- \t") for line in text.splitlines() if line.strip()]
#                         return bullets[0] if len(bullets) > 0 else "", bullets[1] if len(bullets) > 1 else ""

#                     obs_summary = extract_bullets(section_map.get("Observations", ""))
#                     imp_summary = extract_bullets(section_map.get("Improvements", ""))
#                     unit_obs, unit_imp = extract_metric_bullets(section_map.get("Unit Growth", ""))
#                     asp_obs, asp_imp = extract_metric_bullets(section_map.get("ASP", ""))
#                     sales_obs, sales_imp = extract_metric_bullets(section_map.get("Sales", ""))
#                     profit_obs, profit_imp = extract_metric_bullets(section_map.get("Profit", ""))
#                     unitprof_obs, unitprof_imp = extract_metric_bullets(section_map.get("Unit Profitability", ""))

#                     combined_text = f"""Details for {row['product_name']}

#         Observations
#         {obs_summary}

#         Improvements
#         {imp_summary}

#         Unit Growth
#         • {unit_obs}
#         • {unit_imp}

#         ASP
#         • {asp_obs}
#         • {asp_imp}

#         Sales
#         • {sales_obs}
#         • {sales_imp}

#         Profit
#         • {profit_obs}
#         • {profit_imp}

#         Unit Profitability
#         • {unitprof_obs}
#         • {unitprof_imp}
#         """
#                     return pd.Series([
#                         content, obs_summary, unit_obs, asp_obs, sales_obs, profit_obs, unitprof_obs,
#                         imp_summary, unit_imp, asp_imp, sales_imp, profit_imp, unitprof_imp,
#                         combined_text
#                     ])

#                 except Exception as e:
#                     print(f"❌ AI error for product {row['product_name']}: {str(e)}")
#                     return pd.Series(["AI Error"] * 15)








#         df[[ 
#     'raw_ai_response', 'observation_summary', 
#     'observation_unit_growth', 'observation_asp', 'observation_sales', 'observation_profit', 'observation_unit_profitability',
#     'improvement_summary', 
#     'improvement_unit_growth', 'improvement_asp', 'improvement_sales', 'improvement_profit', 'improvement_unit_profitability',
#     'combined_text'
# ]] = df.apply(analyze_row, axis=1)


#         df['footnote'] = df['product_name'].apply(lambda name: generate_trend_footnote(name, history_map.get(name, [])))

#         percentage_cols = [
#             'unit_increase', 'asp_percentag', 'sales_percentage', 'profit_change',
#             'unit_wise_profitability_percentage', 'sales_mix_percentage',
#             'unit_wise_amazon_fee_percentage', 'profit_mix_percentage'
#         ]
#         for col in percentage_cols:
#             if col in df.columns:
#                 df[col] = df[col].apply(lambda x: round(x, 2) if pd.notnull(x) else x)

#         return jsonify({
#             'currency': currency_info,
#             'data': df.to_dict(orient='records'),
#             'data_status': 'success',
#             'analysis_month': current_month,
#             'analysis_year': current_year,
#             'previous_month': prev_month,
#             'previous_year': prev_year
#         })

#     except Exception as e:
#         return jsonify({
#             'error': f'Failed to process improvement suggestion: {str(e)}',
#             'currency': currency_info if 'currency_info' in locals() else None,
#             'data_status': 'general_error'
#         }), 500

    
# ################################################################################################################

# def analyze_sku_patterns(user_id, country):
#     """
#     Analyze SKU activity across all historical tables for a user and country.
#     Returns:
#         - new_skus: SKUs seen for the first time in the latest month.
#         - resurrected_skus: SKUs that reappeared after months of inactivity.
#     """
#     inspector = inspect(engine)
#     all_tables = inspector.get_table_names()
#     pattern = re.compile(f"skuwisemonthly_{user_id}_{country}_(\\w+)(\\d{{4}})$")

#     sku_history = {}
#     month_table_map = {}

#     for table in all_tables:
#         match = pattern.search(table)
#         if match:
#             month_str, year = match.groups()
#             try:
#                 month = list(calendar.month_name).index(month_str.capitalize())
#                 if month == 0:
#                     continue
#                 date_key = f"{int(year):04d}{month:02d}"
#                 month_table_map[date_key] = table
#             except ValueError:
#                 continue

#     sorted_keys = sorted(month_table_map.keys())
#     if not sorted_keys:
#         return {"new_skus": [], "resurrected_skus": []}

#     for key in sorted_keys:
#         table = month_table_map[key]
#         try:
#             df = pd.read_sql_query(f'SELECT DISTINCT sku FROM "{table}"', con=engine)
#             sku_set = set(df["sku"].dropna().astype(str))
#             sku_history[key] = sku_set
#         except Exception as e:
#             sku_history[key] = set()

#     latest_key = sorted_keys[-1]
#     latest_skus = sku_history.get(latest_key, set())
#     all_prev_skus = set().union(*[sku_history[k] for k in sorted_keys[:-1]])
#     new_skus = latest_skus - all_prev_skus

#     resurrected_skus = set()
#     for sku in latest_skus:
#         seen_recently = False
#         for key in reversed(sorted_keys[:-1]):
#             if sku in sku_history[key]:
#                 seen_recently = True
#                 break
#         if not seen_recently:
#             resurrected_skus.add(sku)

#     return {
#         "new_skus": list(new_skus),
#         "resurrected_skus": list(resurrected_skus),
#     }


# @improvement_bp.route('/sku_pattern_improvement', methods=['POST'])
# def sku_pattern_improvement():
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     if not request.is_json:
#         return jsonify({'error': 'Request must be in JSON format'}), 400

#     json_data = request.get_json()
#     country = json_data.get('country', '').lower().strip()
#     if not country:
#         return jsonify({'error': 'Country is required'}), 400

#     try:
#         currency_info = get_currency_info(country)
#         sku_changes = analyze_sku_patterns(user_id, country)
#         new_skus = sku_changes["new_skus"]
#         resurrected_skus = sku_changes["resurrected_skus"]
#         new_and_resurrected_skus = new_skus + resurrected_skus

#         inspector = inspect(engine)
#         all_tables = inspector.get_table_names()
#         base_pattern = f"skuwisemonthly_{user_id}_{country}_"
#         matched_tables = [t for t in all_tables if t.startswith(base_pattern)]

#         valid_tables = []
#         for t in matched_tables:
#             match = re.search(r'_([a-zA-Z]+)(\d{4})$', t)
#             if match:
#                 month_str, year_str = match.group(1).capitalize(), match.group(2)
#                 try:
#                     month_num = list(calendar.month_name).index(month_str)
#                     date_obj = datetime(int(year_str), month_num, 1)
#                     valid_tables.append((t, date_obj))
#                 except ValueError:
#                     continue

#         valid_tables.sort(key=lambda x: x[1], reverse=True)
#         if not valid_tables:
#             return jsonify({'data_status': 'no_data_found'}), 200

#         latest_table, latest_date = valid_tables[0]
#         current_month = latest_date.strftime("%B")
#         current_year = latest_date.strftime("%Y")
#         previous_date = latest_date - timedelta(days=1)
#         prev_month = previous_date.strftime("%B")
#         prev_year = previous_date.strftime("%Y")

#         if not new_and_resurrected_skus:
#             return jsonify({
#                 'currency': currency_info,
#                 'data_status': 'stable_business',
#                 'message': 'Great news! There are no newly introduced or resurrected SKUs this month. Your business seems stable and consistent.',
#                 'new_skus': [],
#                 'resurrected_skus': [],
#                 'analysis_month': current_month,
#                 'analysis_year': current_year,
#                 'previous_month': prev_month,
#                 'previous_year': prev_year
#             }), 200

#         # Query all relevant fields
#         query = f"""
#             SELECT sku, product_name,
#                    unit_growth, asp_growth, sales_growth, profit_growth,
#                    unit_wise_profitability_growth, sales_mix,
#                    unit_increase, asp_percentag, sales_percentage, 
#                    unit_wise_profitability_percentage, profit_change,
#                    sales_mix_percentage, unit_wise_amazon_fee_percentage,
#                    profit_mix_percentage, sales_mix_growth,
#                    amazon_fee_growth, profit_mix_growth
#             FROM "{latest_table}"
#         """
#         df_all = pd.read_sql_query(text(query), engine)
#         df_all = df_all.dropna(subset=['product_name', 'sales_mix'])

#         df_filtered = df_all[df_all['sku'].astype(str).isin(new_and_resurrected_skus)]
#         if df_filtered.empty:
#             return jsonify({
#                 'currency': currency_info,
#                 'data_status': 'no_skus_found',
#                 'message': 'No matching records found for new or resurrected SKUs.',
#                 'new_skus': new_skus,
#                 'resurrected_skus': resurrected_skus,
#                 'analysis_month': current_month,
#                 'analysis_year': current_year,
#                 'previous_month': prev_month,
#                 'previous_year': prev_year
#             }), 200

#         # Add SKU Type
#         df_filtered['sku_type'] = df_filtered['sku'].apply(
#             lambda sku: 'New' if sku in new_skus else 'Resurrected'
#         )

#         # Override all growth fields with sku_type if applicable
#         override_cols = [
#             'unit_growth', 'asp_growth', 'sales_growth', 'profit_growth',
#             'unit_wise_profitability_growth', 'unit_increase', 'asp_percentag',
#             'sales_percentage', 'profit_change', 'unit_wise_profitability_percentage',
#             'sales_mix_percentage', 'unit_wise_amazon_fee_percentage', 'profit_mix_percentage',
#             'sales_mix_growth', 'amazon_fee_growth', 'profit_mix_growth'
#         ]

#         for col in override_cols:
#             if col in df_filtered.columns:
#                 df_filtered[col] = df_filtered.apply(
#                     lambda row: f"{row['sku_type']} Product" if row['sku_type'] in ['New', 'Resurrected'] else row[col],
#                     axis=1
#                 )

#         # AI Suggestion Logic
#         def analyze_row(row):
#             try:
#                 feedback = improvment.query.filter_by(
#                     user_id=user_id,
#                     country=country,
#                     product=row['product_name']
#                 ).order_by(improvment.id.desc()).first()

#                 feedback_summary = ""
#                 if feedback:
#                     sentiment = "positive" if feedback.feedback_type == "like" else "critical"
#                     feedback_summary = f"Past user feedback ({sentiment}): {feedback.feedback_text}\n\n"

#                 prompt = (
#                     f"{feedback_summary}"
#                     f"Product: {row['product_name']}\n"
#                     f"Performance Summary:\n"
#                     f"- Unit Growth: {row['unit_growth']}\n"
#                     f"- ASP Growth: {row['asp_growth']}\n"
#                     f"- Sales Growth: {row['sales_growth']}\n"
#                     f"- CM1 Profit: {row['profit_growth']}\n"
#                     f"- Unit-wise Profitability Growth: {row['unit_wise_profitability_growth']}\n\n"
#                     f"Note: This is a {row['sku_type']} Product introduced this month.\n\n"
#                     "Write output in bullet points:\n- Observation: <short summary>\n- Improvement: <specific suggestion>"
#                 )

#                 response = openai.ChatCompletion.create(
#                     model="gpt-4",
#                     messages=[
#                         {"role": "system", "content": "You are a retail analytics expert. Be concise and helpful."},
#                         {"role": "user", "content": prompt}
#                     ],
#                     max_tokens=100,
#                     temperature=0.4
#                 )

#                 content = response['choices'][0]['message']['content']
#                 match = re.findall(r'-\s*(Observation|Improvement)[:\-]?\s*(.*)', content, re.IGNORECASE)
#                 if match:
#                     obs = next((m[1].strip() for m in match if m[0].lower() == "observation"), "Missing Observation")
#                     imp = next((m[1].strip() for m in match if m[0].lower() == "improvement"), "Missing Improvement")
#                 else:
#                     obs, imp = "Missing Observation", "Missing Improvement"

#                 return pd.Series([obs, imp])
#             except Exception as e:
#                 return pd.Series(["AI Summary Error", "AI Suggestion Error"])

#         df_filtered[['observation', 'improvement']] = df_filtered.apply(analyze_row, axis=1)

#         # Add 3-month history footnotes
#         history_map = collect_3_month_history(valid_tables, engine)
#         df_filtered['footnote'] = df_filtered['product_name'].apply(
#             lambda name: generate_trend_footnote(name, history_map.get(name, []))
#         )

#         # Round numerical percentage columns
#         percentage_cols = [
#             'unit_increase', 'asp_percentag', 'sales_percentage',
#             'profit_change', 'unit_wise_profitability_percentage',
#             'sales_mix_percentage', 'unit_wise_amazon_fee_percentage',
#             'profit_mix_percentage'
#         ]
#         for col in percentage_cols:
#             if col in df_filtered.columns:
#                 df_filtered[col] = df_filtered[col].apply(
#                     lambda x: round(x, 2) if isinstance(x, (int, float)) and not isinstance(x, bool) else x
#                 )

#         return jsonify({
#             'currency': currency_info,
#             'data_status': 'success',
#             'analysis_month': current_month,
#             'analysis_year': current_year,
#             'previous_month': prev_month,
#             'previous_year': prev_year,
#             'sku_insights': df_filtered.to_dict(orient='records'),
#             'new_skus': new_skus,
#             'resurrected_skus': resurrected_skus
#         })

#     except Exception as e:
#         return jsonify({
#             'error': f'Failed to analyze SKU patterns: {str(e)}',
#             'currency': currency_info if 'currency_info' in locals() else None,
#             'data_status': 'general_error'
#         }), 500




# ###################################################################################################################

# def get_bottom_20_percent_skus(sku_data, exclude_skus=None):
#     if not sku_data:
#         return []

#     exclude_skus = set(exclude_skus or [])

#     # Filter out 'TOTAL' row and excluded SKUs
#     filtered_skus = [
#         sku for sku in sku_data 
#         if sku['product_name'].strip().lower() != 'total'
#         and sku['sku'] not in exclude_skus
#     ]

#     # Sort descending by sales_mix
#     sorted_skus = sorted(filtered_skus, key=lambda x: x['sales_mix'], reverse=True)

#     cumulative = 0.0
#     top_skus = []

#     for sku in sorted_skus:
#         cumulative += sku['sales_mix']
#         top_skus.append(sku)
#         if cumulative >= 80.0:
#             break

#     # Exclude top 80% and return rest
#     top_sku_names = {sku['product_name'] for sku in top_skus}
#     bottom_skus = [
#         sku for sku in filtered_skus 
#         if sku['product_name'] not in top_sku_names
#     ]

#     return bottom_skus


# @improvement_bp.route('/improvement_tab_suggestion_bottom', methods=['POST'])
# def improvement_tab_suggestion_bottom():
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     if not request.is_json:
#         return jsonify({'error': 'Request must be in JSON format'}), 400

#     json_data = request.get_json()
#     country = json_data.get('country', '').lower().strip()
#     if not country:
#         return jsonify({'error': 'Country is required'}), 400

#     try:
#         currency_info = get_currency_info(country)
#         inspector = inspect(engine)
#         all_tables = inspector.get_table_names()
#         base_pattern = f"skuwisemonthly_{user_id}_{country}_"
#         matched_tables = [t for t in all_tables if t.startswith(base_pattern)]

#         valid_tables = []
#         for t in matched_tables:
#             match = re.search(r'_([a-zA-Z]+)(\d{4})$', t)
#             if match:
#                 month_str, year_str = match.group(1).capitalize(), match.group(2)
#                 try:
#                     month_num = list(calendar.month_name).index(month_str)
#                     date_obj = datetime(int(year_str), month_num, 1)
#                     valid_tables.append((t, date_obj))
#                 except ValueError:
#                     continue

#         valid_tables.sort(key=lambda x: x[1], reverse=True)
#         if not valid_tables:
#             return jsonify({
#                 'data_status': 'no_data_found',
#                 'currency': currency_info,
#                 'message': f'No valid tables found for user {user_id} in country {country}.'
#             }), 200

#         latest_table, latest_date = valid_tables[0]
#         current_month = latest_date.strftime("%B")
#         current_year = latest_date.strftime("%Y")
#         previous_date = latest_date - timedelta(days=1)
#         prev_month = previous_date.strftime("%B")
#         prev_year = previous_date.strftime("%Y")

#         query = f"""
#             SELECT 
#                 product_name, sku,
#                 unit_growth, asp_growth, sales_growth, profit_growth,
#                 unit_wise_profitability_growth, sales_mix,
#                 unit_increase, asp_percentag, sales_percentage,
#                 unit_wise_profitability_percentage, profit_change,
#                 sales_mix_percentage, unit_wise_amazon_fee_percentage,
#                 profit_mix_percentage, sales_mix_growth,
#                 amazon_fee_growth, profit_mix_growth
#             FROM "{latest_table}"
#         """
#         df_all = pd.read_sql_query(text(query), engine)
#         df_all = df_all.dropna(subset=['product_name', 'sales_mix'])

#         sku_data = df_all.to_dict(orient='records')
#         sku_changes = analyze_sku_patterns(user_id, country)
#         excluded_skus = set(sku_changes.get("new_skus", []) + sku_changes.get("resurrected_skus", []))

#         bottom_skus = get_bottom_20_percent_skus(sku_data, exclude_skus=excluded_skus)
#         df = pd.DataFrame(bottom_skus)

#         history_map = collect_3_month_history(valid_tables, engine)

#         def format_latest_metrics(row):
#             metrics = {
#                 "Unit Growth": row.get("unit_increase"),
#                 "ASP": row.get("asp_percentag"),
#                 "Sales": row.get("sales_percentage"),
#                 "Profit": row.get("profit_change"),
#                 "Unit Profitability": row.get("unit_wise_profitability_percentage"),
#             }
#             result = []
#             for key, val in metrics.items():
#                 try:
#                     formatted_val = f"{float(val):.2f}%" if val is not None else "Data not available"
#                 except Exception:
#                     formatted_val = "Data not available"
#                 result.append(f"- {key}: {formatted_val}")
#             return "\n".join(result)

#         SYSTEM_PROMPT = """You are a concise but insightful retail analytics expert.

# When analyzing historical product performance:
# - Always use exact month transitions (e.g., 'Apr → May', 'May → Jun').
# - Compare at least two periods (e.g., 'Sales rose in April, then fell in May and June').
# - Avoid vague phrases like 'declined recently'. Be specific.

# Example Observation:
# - Unit Growth: Rose +12% from Mar → Apr, held steady in Apr → May, then dropped -18% in May → Jun.
# - ASP: Stable around +0.2% across all months.
# - Profit: Gained +10% in Apr and May, dropped -20% in June.

# Example Improvement:
# - Unit Growth: Reintroduce April's campaign to recover from the -18% May → Jun dip.
# - ASP: Test a 3% increase since ASP was flat and demand dropped.
# - Profit: Investigate June’s cost spikes to restore margins.
# """

#         def analyze_row(row):
#             with current_app.app_context():  # ✅ Required for DB access inside threads
#                 try:
#                     feedback = improvment.query.filter_by(
#                         user_id=user_id,
#                         country=country,
#                         product=row['product_name']
#                     ).order_by(improvment.id.desc()).first()

#                     feedback_summary = ""
#                     if feedback:
#                         feedback_type = "positive" if feedback.feedback_type == "like" else "critical"
#                         feedback_summary = f"Past user feedback ({feedback_type}): {feedback.feedback_text}\n\n"

#                     product_history = history_map.get(row['product_name'], [])
#                     usable_deltas = (
#                         product_history[-3:] if len(product_history) >= 3
#                         else product_history[-2:] if len(product_history) == 2
#                         else product_history[-1:]
#                     )

#                     month_transitions = {
#                         "January": "Dec → Jan", "February": "Jan → Feb", "March": "Feb → Mar",
#                         "April": "Mar → Apr", "May": "Apr → May", "June": "May → Jun",
#                         "July": "Jun → Jul", "August": "Jul → Aug", "September": "Aug → Sep",
#                         "October": "Sep → Oct", "November": "Oct → Nov", "December": "Nov → Dec"
#                     }

#                     if usable_deltas:
#                         labeled_deltas = [
#                             f"- {month_transitions.get(entry['month'], entry['month'])}: "
#                             f"Units {entry['unit_growth']}%, ASP {entry['asp_growth']}%, "
#                             f"Sales {entry['sales_growth']}%, Profit {entry['profit_growth']}%"
#                             for entry in usable_deltas
#                         ]
#                         history_summary = "\n".join(labeled_deltas)
#                     else:
#                         history_summary = "Not enough historical data."

#                     latest_metrics_formatted = format_latest_metrics(row)

#                     prompt = f"""
#         {feedback_summary}
# Product: {row['product_name']}

# 📉 Historical Performance Deltas:
# {history_summary}

# 📈 Latest Month Detailed Metrics:
# {latest_metrics_formatted}

# ✍️ Write the analysis in the following exact format:

# Details for {row['product_name']}

# Observations
# 1. Write 2-3 numbered bullets summarizing overall performance. Each bullet must weave multiple metrics (Unit Growth, Sales, ASP, Profit) together, cite % changes, and use month transitions (e.g., “Apr to May”, “May to Jun”). Highlight the biggest swings; avoid per-metric repetition here.

# Improvements
# 1. Write 2-3 numbered bullets with strategy-level actions. Do not list every metric again. Tie each recommendation to the patterns and months above (e.g., “Reverse the Jun unit drop by…”).

# Unit Growth
# • Observation: Describe the multi-month trend with explicit month transitions and % shifts. Call out sharp inflections.
# • Improvement: One concrete lever to fix declines or scale gains (e.g., channel mix, promo cadence). Reference months if relevant.

# ASP
# • Observation: Precisely state how ASP moved month to month with % values—no vague terms like “slight”.
# • Improvement: Actionable pricing tactic (bundles, premium SKU, controlled discount window, A/B price test).

# Sales
# • Observation: Explain Sales changes and link them to Unit Growth and/or ASP. Mention every recent month-to-month shift.
# • Improvement: Focused actions to lift Sales (campaign timing, replicate high-growth periods, channel expansion).

# Profit
# • Observation: Note whether Profit followed Sales or diverged. Use % and months (e.g., “Profit +12% in May despite flat Sales”).
# • Improvement: Margin-focused tactic (lower COGS, bundle for ASP lift, replicate profitable price windows).

# Unit Profitability
# • Observation: State how per-unit profit moved across the same range—rising, falling, or stable (quantify even small moves).
# • Improvement: Steps to maintain/boost it (supplier renegotiation, drop low-margin variants, packaging/fulfillment efficiency).

# Use this exact structure and formatting but do not give prefix observation and improvements in the final output. Tone must reflect a senior business analyst writing to a leadership team. No extra summaries or generic advice — base all insights on provided data only, never invent missing numbers.

#         """

#                     response = openai.ChatCompletion.create(
#                         model="gpt-4",
#                         messages=[
#                             {"role": "system", "content": SYSTEM_PROMPT},
#                             {"role": "user", "content": prompt}
#                         ],
#                         max_tokens=600,
#                         temperature=0.3,
#                     ) 
                    

#                     content = response['choices'][0]['message']['content']

#                     section_headers = [
#                         "Observations", "Improvements",
#                         "Unit Growth", "ASP", "Sales", "Profit", "Unit Profitability"
#                     ]
#                     parts = re.split(r'^\s*(' + '|'.join(re.escape(h) for h in section_headers) + r')\s*$',
#                                     content.strip(), flags=re.MULTILINE)

#                     section_map = {}
#                     i = 1
#                     while i < len(parts):
#                         label = parts[i].strip()
#                         body = parts[i + 1].strip() if i + 1 < len(parts) else ''
#                         section_map[label] = body
#                         i += 2

#                     def extract_bullets(text):
#                         return "\n".join([l.strip("•- \t") for l in text.splitlines() if l.strip()])

#                     def extract_metric_bullets(text):
#                         bullets = [line.strip("•- \t") for line in text.splitlines() if line.strip()]
#                         return bullets[0] if len(bullets) > 0 else "", bullets[1] if len(bullets) > 1 else ""

#                     obs_summary = extract_bullets(section_map.get("Observations", ""))
#                     imp_summary = extract_bullets(section_map.get("Improvements", ""))
#                     unit_obs, unit_imp = extract_metric_bullets(section_map.get("Unit Growth", ""))
#                     asp_obs, asp_imp = extract_metric_bullets(section_map.get("ASP", ""))
#                     sales_obs, sales_imp = extract_metric_bullets(section_map.get("Sales", ""))
#                     profit_obs, profit_imp = extract_metric_bullets(section_map.get("Profit", ""))
#                     unitprof_obs, unitprof_imp = extract_metric_bullets(section_map.get("Unit Profitability", ""))

#                     combined_text = f"""Details for {row['product_name']}

#         Observations
#         {obs_summary}

#         Improvements
#         {imp_summary}

#         Unit Growth
#         • {unit_obs}
#         • {unit_imp}

#         ASP
#         • {asp_obs}
#         • {asp_imp}

#         Sales
#         • {sales_obs}
#         • {sales_imp}

#         Profit
#         • {profit_obs}
#         • {profit_imp}

#         Unit Profitability
#         • {unitprof_obs}
#         • {unitprof_imp}
#         """
#                     return pd.Series([
#                         content, obs_summary, unit_obs, asp_obs, sales_obs, profit_obs, unitprof_obs,
#                         imp_summary, unit_imp, asp_imp, sales_imp, profit_imp, unitprof_imp,
#                         combined_text
#                     ])

#                 except Exception as e:
#                     return pd.Series(["AI Error"] * 15)

#         df[[
#             'raw_ai_response', 'observation_summary',
#             'observation_unit_growth', 'observation_asp', 'observation_sales', 'observation_profit', 'observation_unit_profitability',
#             'improvement_summary',
#             'improvement_unit_growth', 'improvement_asp', 'improvement_sales', 'improvement_profit', 'improvement_unit_profitability',
#             'combined_text'
#         ]] = df.apply(analyze_row, axis=1)

#         df['footnote'] = df['product_name'].apply(lambda name: generate_trend_footnote(name, history_map.get(name, [])))

#         percentage_cols = [
#             'unit_increase', 'asp_percentag', 'sales_percentage',
#             'profit_change', 'unit_wise_profitability_percentage',
#             'sales_mix_percentage', 'unit_wise_amazon_fee_percentage',
#             'profit_mix_percentage'
#         ]
#         for col in percentage_cols:
#             if col in df.columns:
#                 df[col] = df[col].apply(lambda x: round(x, 2) if pd.notnull(x) else x)

#         return jsonify({
#             'currency': currency_info,
#             'data': df.to_dict(orient='records'),
#             'data_status': 'success',
#             'analysis_month': current_month,
#             'analysis_year': current_year,
#             'previous_month': prev_month,
#             'previous_year': prev_year
#         })

#     except Exception as e:
#         return jsonify({
#             'error': f'Failed to process bottom SKU suggestion: {str(e)}',
#             'currency': currency_info if 'currency_info' in locals() else None,
#             'data_status': 'general_error'
#         }), 500


# ###############################################################################################################################################################

# # Updated API Route for handling row feedback
# @improvement_bp.route('/row-feedback', methods=['POST'])
# def save_row_feedback():
#     auth_header = request.headers.get('Authorization')
#     if not auth_header or not auth_header.startswith('Bearer '):
#         return jsonify({'error': 'Authorization token is missing or invalid'}), 401

#     token = auth_header.split(' ')[1]
#     try:
#         payload, user_id, member_id = get_effective_user_id_from_token(token)
#     except jwt.ExpiredSignatureError:
#         return jsonify({'error': 'Token has expired'}), 401
#     except jwt.InvalidTokenError:
#         return jsonify({'error': 'Invalid token'}), 401

#     if not request.is_json:
#         return jsonify({'error': 'Request must be in JSON format'}), 400

#     data = request.get_json()

#     country = (data.get('country') or '').strip().lower()
#     row_index = data.get('rowIndex')
#     tab_number = data.get('tab')
#     feedback_type = (data.get('type') or '').strip().lower()  
#     feedback_text = (data.get('text') or '').strip()
#     product_data = data.get('productData') or {}

#     if not country or row_index is None or tab_number is None or feedback_type not in {'like', 'dislike'}:
#         return jsonify({'error': 'Missing or invalid required fields'}), 400

#     # Enforce DB column limits and booleans
#     feedback_text = feedback_text[:500]
#     is_liked = feedback_type == 'like'
#     is_disliked = feedback_type == 'dislike'

#     # save_row_feedback()
#     product_data = data.get('productData') or {}
#     response_text = (
#         product_data.get('combined_text') or
#         product_data.get('raw_ai_response') or
#         ''
#     ).strip()

#     # (optional) drop or bump the cap; TEXT can handle large payloads
#     MAX_RESPONSE = 5_000_000  # or remove this guard entirely
#     if len(response_text) > MAX_RESPONSE:
#         response_text = response_text[:MAX_RESPONSE]

#     try:
#         rec = improvment(
#             user_id=user_id,
#             country=country,
#             product=(product_data.get('product_name') or '')[:255],
#             response=response_text,  # now the combined text users see
#             feedback_type=feedback_type,
#             feedback_text=feedback_text,
#             is_liked=is_liked,
#             is_disliked=is_disliked,
#             tab_number=int(tab_number),
#             row_index=int(row_index),
#         )

#         db.session.add(rec)
#         db.session.commit()

#         return jsonify({
#             'success': True,
#             'message': 'Feedback saved successfully',
#             'feedback_id': rec.id
#         }), 200

#     except Exception as e:
#         db.session.rollback()
#         return jsonify({'error': f'Failed to save feedback: {str(e)}'}), 500
