# Streamlit
import streamlit as st
# Funciones necesarias
from utils import snowpark

# Formato de página
st.set_page_config(
  page_title = "Home",
  page_icon = "🏠",
  layout = "wide",
  initial_sidebar_state = "expanded",
  menu_items = {
    'Get Help': 'https://www.hiberus.com/tecnologia/snowflake-ld',
    'Report a bug': None,
    'About': "This is an *extremely* cool app powered by Snowpark and Streamlit"
  }
)

# Imagenes
image_path = "https://raw.githubusercontent.com/mmaicasm/app_publica/main/streamlit_src/snowflake-logo.jpg"

# Ocultar índices de tablas
hide_table_row_index = """
  <style>
  thead tr th:first-child {display:none}
  tbody th {display:none}
  </style>
  """
st.markdown(hide_table_row_index, unsafe_allow_html = True)

# Secciones de la App (Containers)
st.title('Home')
st.subheader('Conexión a Snowflake mediante Snowpark')

# Inicializar estados
if 'logged' not in st.session_state:
  st.session_state['logged'] = False
  st.session_state['user']  = ''
  st.session_state['role']  = ''
  st.session_state['warehouse']  = ''

# Widget manual
with st.form(key = "login"):
  
  user = st.text_input(placeholder = 'usuario@hiberus.com', label = 'Usuario', disabled = True)
  password = st.text_input(type = 'password', label = 'Contraseña', disabled = True)
  
  #login = st.form_submit_button("Conectar")
  guest = st.form_submit_button("Acceder como invitado")
  
  if guest:
    # Crear sesión
    with st.spinner('Conectando a Snowflake...'):
      session = snowpark.guest_connect()
    
    # Informar conexión correcta
    st.success('Sesión confirmada!')
    st.snow()
    
    # Mostrar parámetros de la sesión
    st.write('Parámetros de la sesión:')
    st.table(session.sql('select current_user(), current_role()').collect())