# Streamlit
import streamlit as st
# Snowpark
from snowflake.snowpark.session import Session
from snowflake.snowpark import DataFrame
# Librerias necesarias
import pandas as pd
import random as ra

# Parámetros de conexión
guest_connection_parameters = {
  "account": st.secrets["snowflake_account"],
  "user": st.secrets["guest_user"],
  "password": st.secrets["guest_password"],
  "role": "STREAMLIT_READ",
  "warehouse": "STREAMLIT_WH"
}

# Funciones con memoria
@st.cache_resource(show_spinner = False)
def guest_connect():
  # Se randomiza el usuario si aún no tiene
  if st.session_state['user'] == '':
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
    st.error('Usuario y/o contraseña erróneos')
    st.stop()
    
  return session

# function - run sql query and return data
@st.cache_data(show_spinner = False)
def query_snowflake(_session, sql) -> pd.DataFrame:

  try:
    df = _session.sql(sql).to_pandas()
      
  except Exception as e:
    st.error(e)
    st.write(e)
    st.write(e.error_code)
    st.stop()

  return df

# Función para cargar los datos según los widgets
@st.cache_data(show_spinner = False)
def load_data(_session, prediction) -> pd.DataFrame:
  
  if prediction[0] == 'X':
    table = 'xxxxxxxx'
  elif prediction[0] == 'X':
    table = 'xxxxxxxx'
  elif prediction[0] == 'X':
    table = 'xxxxxxxx'
  else:
    st.error('Modelo no reconococido')
    
  if prediction[3] != 'Unisex':
    filtro_gen = f' AND GENERO = {prediction[3]}'
  else:
    filtro_gen = ''
    
  try:
    df = _session.sql(f'SELECT XXXXX FROM {table} WHERE PAIS in ({prediction[1]}) AND TIPO_PRENDA = {prediction[2]} {filtro_gen}').to_pandas()
    #df['DATE'] = pd.to_datetime(df['DATE'])
  except Exception as e:
    st.error(e)
    return e
  
  return df