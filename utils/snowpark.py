# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
# Librerias necesarias
import pandas as pd
import random as ra

# Parámetros de conexión
guest_connection_parameters = {
  "account": st.secrets["snowflake_account"],
  "user": st.secrets["guest_user"],
  "password": st.secrets["guest_password"],
  "role": "STREAMLIT_READ",
  "warehouse": "STREAMLIT_WH",
  "client_session_keep_alive": True
}

# Funciones con memoria
@st.cache_resource(show_spinner = False)
def guest_connect():
  # Se randomiza el usuario si aún no tiene
  if guest_connection_parameters["user"] == 'STREAMLIT_GUEST_':
    n = ra.randint(1,10)
    guest_connection_parameters["user"] += str(n)
  
  try:
    # Se crea la conexión
    session = Session.builder.configs(guest_connection_parameters).create()
    
    # Se actualiza la cache
    st.session_state['logged'] = True
    st.session_state['user'] = guest_connection_parameters["user"]
    st.session_state['role'] = session.get_current_role()
    st.session_state['warehouse'] = guest_connection_parameters["warehouse"]
    if 'session' not in st.session_state:
      st.session_state['session'] = session
  
  except Exception as e:
    st.error(e)
    st.stop()
    
  return session

# function - run sql query and return data
@st.cache_data(show_spinner = False)
def query_snowflake(_session, sql) -> pd.DataFrame:

  try:
    df = _session.sql(sql).to_pandas()
      
  except Exception as e:
    if e.error_code == '1304':
      _session.close()
      st.cache_resource.clear()
      st.warning('La sesión ha caducado, por favor recarga la página')
    else:
      st.error(e)
    st.stop()

  return df

# Función para cargar los datos según los widgets
@st.cache_data(show_spinner = False)
def load_data(_session, prediction) -> pd.DataFrame:
  
  table = 'EVENTO_SNOWFLAKE.PUBLIC_DATA.PREDICTION_DEMO'
  
  if len(prediction[1]) > 1:
    filtro = f"PAIS = '{prediction[0]}' AND PRODUCTO in {tuple(prediction[1])}"
  else:
    filtro = f"PAIS = '{prediction[0]}' AND PRODUCTO = '{prediction[1][0]}'"
  cols = 'YEAR, MONTH, MES, PREDICTION, PRODUCTO'
  
  query = f'SELECT YEAR, MONTH, MES, PREDICTION, PRODUCTO, SUM(UNIDADES) AS UNIDADES FROM {table} WHERE {filtro} GROUP BY {cols} ORDER BY {cols}'
    
  try:
    df = _session.sql(query).to_pandas()
  except Exception as e:
    st.error(e)
    return e
  
  return df