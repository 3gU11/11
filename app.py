import streamlit as st

from core.bootstrap import initialize_app
from views.auth import login_form
from views.router import render_current_page
from views.sidebar import render_sidebar

initialize_app()

if st.session_state.current_user is None:
    login_form()
    st.stop()

render_sidebar()
render_current_page(st.session_state.page)
