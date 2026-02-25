import streamlit as st
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Turnos Farmacia", layout="wide")
st.title("🧾 Turnos Farmacia (panel sencillo)")

@st.cache_resource
def engine():
    return create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)

eng = engine()

def read_df(sql, params=None):
    with eng.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})

def exec_sql(sql, params=None):
    with eng.begin() as c:
        c.execute(text(sql), params or {})

tab1, tab2, tab3 = st.tabs(["⚙️ Configuración y personal", "🗓️ Generador semanal", "📊 Dashboard mensual"])

with tab1:
    st.subheader("1) Personas")
    with st.form("add_person", clear_on_submit=True):
        name = st.text_input("Nombre")
        role = st.selectbox("Rol", ["empleada", "encargada"])
        active = st.checkbox("Activa", value=True)
        ok = st.form_submit_button("➕ Guardar")
        if ok and name.strip():
            exec_sql("""
                insert into employees (full_name, role, active)
                values (:name, :role, :active)
            """, {"name": name.strip(), "role": role, "active": active})
            st.success("Guardado.")

    df_emp = read_df("select full_name, role, active from employees order by full_name")
    st.dataframe(df_emp, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("2) Turnos")
    with st.form("add_shift", clear_on_submit=True):
        code = st.text_input("Código (ej: M, T)")
        name = st.text_input("Nombre")
        start = st.time_input("Hora inicio")
        end = st.time_input("Hora fin")
        required = st.number_input("Personas necesarias", min_value=1, value=2)
        weight = st.number_input("Peso justicia (Tarde más alto)", min_value=1, value=2)
        active = st.checkbox("Activo", value=True)
        ok2 = st.form_submit_button("💾 Guardar turno")
        if ok2 and code.strip() and name.strip():
            exec_sql("""
                insert into shift_types (code, name, start_time, end_time, required_staff, fairness_weight, active)
                values (:code, :name, :start, :end, :req, :w, :active)
                on conflict (code) do update set
                  name=excluded.name,
                  start_time=excluded.start_time,
                  end_time=excluded.end_time,
                  required_staff=excluded.required_staff,
                  fairness_weight=excluded.fairness_weight,
                  active=excluded.active
            """, {"code": code.strip(), "name": name.strip(), "start": str(start), "end": str(end),
                  "req": int(required), "w": int(weight), "active": active})
            st.success("Turno guardado.")

    df_shift = read_df("""
        select code, name, start_time, end_time, required_staff, fairness_weight, active
        from shift_types
        order by start_time
    """)
    st.dataframe(df_shift, use_container_width=True, hide_index=True)

    st.divider()
    st.subheader("3) Vacaciones / ausencias")
    emp_list = read_df("select id, full_name from employees where active=true order by full_name")
    shift_list = read_df("select id, name from shift_types where active=true order by start_time")

    if not emp_list.empty:
        emp_name = st.selectbox("Persona", emp_list["full_name"].tolist())
        emp_id = emp_list.loc[emp_list["full_name"] == emp_name, "id"].iloc[0]

        c1, c2 = st.columns(2)
        with c1:
            start_d = st.date_input("Desde", value=date.today())
        with c2:
            end_d = st.date_input("Hasta", value=date.today())

        shift_opts = ["(todo el día)"] + (shift_list["name"].tolist() if not shift_list.empty else [])
        shift_sel = st.selectbox("Turno (opcional)", shift_opts)
        reason = st.text_input("Motivo", value="Vacaciones")

        if st.button("➕ Añadir"):
            shift_id = None
            if shift_sel != "(todo el día)" and not shift_list.empty:
                shift_id = shift_list.loc[shift_list["name"] == shift_sel, "id"].iloc[0]
            exec_sql("""
                insert into employee_time_off (employee_id, start_date, end_date, shift_type_id, reason)
                values (:emp, :s, :e, :shift, :r)
            """, {"emp": emp_id, "s": str(start_d), "e": str(end_d), "shift": shift_id, "r": reason})
            st.success("Ausencia guardada.")

with tab2:
    st.subheader("Generar semana")
    week_start = st.date_input("Semana (lunes)", value=date.today())

    if st.button("⚙️ Generar", type="primary"):
        exec_sql("select generate_week_schedule(:w)", {"w": str(week_start)})
        st.success("Semana generada.")

    df = read_df("""
        select sa.work_date, st.name as turno, e.full_name as persona
        from schedule_weeks sw
        join schedule_assignments sa on sa.schedule_week_id = sw.id
        join shift_types st on st.id = sa.shift_type_id
        join employees e on e.id = sa.employee_id
        where sw.week_start = :w
        order by sa.work_date, st.start_time, e.full_name
    """, {"w": str(week_start)})

    st.dataframe(df, use_container_width=True, hide_index=True)

with tab3:
    st.subheader("Dashboard mensual")
    st.write("Cuando generes semanas, aquí veremos resúmenes del mes.")