import streamlit as st
import pandas as pd
import json
from datetime import date, timedelta
from sqlalchemy import create_engine, text
import streamlit.components.v1 as components
 
st.set_page_config(page_title="Turnos Farmacia", layout="wide")
 
# =============================================================================
# CAPA DE DATOS
# Convención de caché:
#   @st.cache_resource  → conexión a BD (singleton por sesión de servidor)
#   @st.cache_data(ttl=300) → datos casi estáticos: turnos, empleados activos
#   @st.cache_data(ttl=30)  → datos operativos: disponibilidad, asignaciones
#
# Todas las funciones de escritura llaman a _invalidate_caches() al final
# para forzar que la siguiente lectura vaya a la BD y no devuelva datos viejos.
# =============================================================================
 
# ---------- Conexión ----------
 
@st.cache_resource
def engine():
    if "DATABASE_URL" not in st.secrets:
        raise KeyError("Falta DATABASE_URL en Secrets (Streamlit Cloud → Settings → Secrets)")
    return create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)
 
eng = engine()
 
def read_df(sql, params=None):
    with eng.connect() as c:
        return pd.read_sql(text(sql), c, params=params or {})
 
def exec_sql(sql, params=None):
    with eng.begin() as c:
        c.execute(text(sql), params or {})
 
def _invalidate_caches():
    """Limpia las cachés estáticas tras cualquier escritura en BD."""
    get_active_shifts.clear()
    get_active_employees.clear()
 
# ---------- Helpers ----------
 
ISO_DOW = {1:"Lun",2:"Mar",3:"Mié",4:"Jue",5:"Vie",6:"Sáb",7:"Dom"}
 
def month_range(any_day_in_month: date):
    start = any_day_in_month.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end
 
def month_start(d: date) -> date:
    return d.replace(day=1)
 
def next_month_start(d: date) -> date:
    ms = month_start(d)
    if ms.month == 12:
        return date(ms.year + 1, 1, 1)
    return date(ms.year, ms.month + 1, 1)
 
# ---------- Lecturas con caché (solo funciones sin parámetros date) ----------
 
@st.cache_data(ttl=300)
def get_active_shifts():
    """Turnos activos. Cambian rarísimo → TTL 5 min."""
    return read_df("""
        select id, code, name, start_time, end_time, required_staff
        from shift_types
        where active=true
        order by start_time
    """)
 
@st.cache_data(ttl=300)
def get_active_employees():
    """Empleadas activas. Cambian rarísimo → TTL 5 min."""
    return read_df("""
        select id, full_name, role
        from employees
        where active=true
        order by full_name
    """)
 
# ---------- Lecturas operativas (sin caché, date no es hasheable de forma fiable) ----------
 
def get_effective_availability_all(work_date: date, iso_dow: int, shift_id: str):
    return read_df("""
        select
          e.id,
          e.full_name,
          coalesce(o.available, w.available, true) as is_available,
          exists (
            select 1 from employee_time_off t
            where t.employee_id=e.id
              and :dt between t.start_date and t.end_date
              and (t.shift_type_id is null or t.shift_type_id=:shift)
          ) as is_time_off
        from employees e
        left join employee_weekly_availability w
          on w.employee_id=e.id and w.iso_dow=:dow and w.shift_type_id=:shift
        left join employee_availability_overrides o
          on o.employee_id=e.id and o.work_date=:dt and o.shift_type_id=:shift
        where e.active=true
        order by e.full_name
    """, {"dt": str(work_date), "dow": iso_dow, "shift": shift_id})
 
def available_employees_for_date_shift(work_date: date, iso_dow: int, shift_id: str):
    return read_df("""
        select e.id, e.full_name
        from employees e
        left join employee_weekly_availability w
          on w.employee_id=e.id and w.iso_dow=:dow and w.shift_type_id=:shift
        left join employee_availability_overrides o
          on o.employee_id=e.id and o.work_date=:dt and o.shift_type_id=:shift
        where e.active=true
          and not exists (
            select 1 from employee_time_off t
            where t.employee_id = e.id
              and :dt between t.start_date and t.end_date
              and (t.shift_type_id is null or t.shift_type_id = :shift)
          )
          and coalesce(o.available, w.available, true) = true
        order by e.full_name
    """, {"dt": str(work_date), "dow": iso_dow, "shift": shift_id})
 
def get_assignments(work_date: date, shift_id: str):
    return read_df("""
        select a.id as assignment_id, a.employee_id, a.active, e.full_name
        from shift_assignments a
        join employees e on e.id = a.employee_id
        where a.work_date=:dt and a.shift_type_id=:shift
        order by e.full_name
    """, {"dt": str(work_date), "shift": shift_id})
 
def get_monthly_shift_counts(month_start_date: date, month_end_date: date) -> dict:
    df = read_df("""
        select employee_id, count(*) as cnt
        from shift_assignments
        where active=true
          and work_date >= :s and work_date < :e
        group by employee_id
    """, {"s": str(month_start_date), "e": str(month_end_date)})
    if df.empty:
        return {}
    return dict(zip(df["employee_id"].tolist(), df["cnt"].tolist()))
 
# ---------- Escrituras (invalidan caché tras ejecutar) ----------
 
def upsert_override(emp_id: str, work_date: date, shift_id: str, available: bool, reason: str = ""):
    exec_sql("""
        insert into employee_availability_overrides (employee_id, work_date, shift_type_id, available, reason)
        values (:e, :dt, :s, :a, :r)
        on conflict (employee_id, work_date, shift_type_id)
        do update set available = excluded.available,
                      reason = excluded.reason
    """, {"e": emp_id, "dt": str(work_date), "s": shift_id, "a": available, "r": reason})
    _invalidate_caches()
 
def upsert_weekly_availability(emp_id, iso_dow, shift_id, available):
    exec_sql("""
        insert into employee_weekly_availability (employee_id, iso_dow, shift_type_id, available)
        values (:e, :d, :s, :a)
        on conflict (employee_id, iso_dow, shift_type_id)
        do update set available = excluded.available
    """, {"e": emp_id, "d": iso_dow, "s": shift_id, "a": available})
    _invalidate_caches()
 
def set_assignment_active(assignment_id: str, active: bool):
    exec_sql("""
        update shift_assignments
        set active=:a
        where id=:id
    """, {"a": active, "id": assignment_id})
    _invalidate_caches()
 
def is_month_closed(ms: date) -> bool:
    df = read_df("select month_start from month_closures where month_start=:m", {"m": str(ms)})
    return not df.empty
 
def close_month(ms: date, closed_by: str = ""):
    exec_sql("""
        insert into month_closures (month_start, closed_by)
        values (:m, :by)
        on conflict (month_start) do nothing
    """, {"m": str(ms), "by": closed_by})
 
def apply_assignments(work_date: date, iso_dow: int, shift_id: str, selected_employee_ids: list):
    existing = read_df("""
        select employee_id, id as assignment_id
        from shift_assignments
        where work_date=:dt and shift_type_id=:shift
    """, {"dt": str(work_date), "shift": shift_id})
 
    existing_ids = set(existing["employee_id"].tolist()) if not existing.empty else set()
    selected_ids = set(selected_employee_ids)
 
    for emp_id in selected_ids:
        exec_sql("""
            insert into shift_assignments (work_date, iso_dow, shift_type_id, employee_id, active)
            values (:dt, :dow, :shift, :emp, true)
            on conflict (work_date, shift_type_id, employee_id)
            do update set active=true
        """, {"dt": str(work_date), "dow": iso_dow, "shift": shift_id, "emp": emp_id})
 
    to_deactivate = list(existing_ids - selected_ids)
    if to_deactivate:
        exec_sql("""
            update shift_assignments
            set active=false
            where work_date=:dt and shift_type_id=:shift and employee_id = any(:arr)
        """, {"dt": str(work_date), "shift": shift_id, "arr": to_deactivate})
    _invalidate_caches()
 
 
# ---------- AUTOASIGNACIÓN INTELIGENTE ----------
 
 
def auto_assign_month(target_month: date, shifts_df: pd.DataFrame, only_empty: bool = True):
    """
    Recorre todos los días del mes y asigna automáticamente cada turno.
 
    Criterio de selección:
      - Solo personas disponibles (disponibilidad semanal + overrides + sin vacaciones).
      - Ordenadas por turnos acumulados en el mes (menor carga primero).
      - Se asignan las primeras `required_staff` personas de esa lista.
      - Si ya hay asignaciones activas en ese turno/día y only_empty=True, se salta.
 
    Devuelve un dict con estadísticas del resultado.
    """
    start, end = month_range(target_month)
    stats = {"cubiertos": 0, "parciales": 0, "sin_personal": 0, "saltados": 0, "total": 0}
 
    # Contadores en memoria para ir actualizando la carga dentro del mismo proceso
    shift_counts = get_monthly_shift_counts(start, end)
 
    d = start
    while d < end:
        iso_dow = d.isoweekday()
        for sh in shifts_df.itertuples(index=False):
            shift_id = str(sh.id)
            req = int(sh.required_staff)
            stats["total"] += 1
 
            # Si only_empty, saltar días que ya tienen asignaciones activas
            if only_empty:
                existing = read_df("""
                    select count(*) as cnt from shift_assignments
                    where work_date=:dt and shift_type_id=:shift and active=true
                """, {"dt": str(d), "shift": shift_id})
                if not existing.empty and int(existing.iloc[0]["cnt"]) > 0:
                    stats["saltados"] += 1
                    continue
 
            # Personas disponibles para este día/turno
            avail = available_employees_for_date_shift(d, iso_dow, shift_id)
            if avail.empty:
                stats["sin_personal"] += 1
                continue
 
            # Ordenar por carga acumulada en el mes (menor primero), desempate por nombre
            avail["_load"] = avail["id"].apply(lambda eid: shift_counts.get(str(eid), 0))
            avail = avail.sort_values(["_load", "full_name"]).reset_index(drop=True)
 
            # Seleccionar hasta required_staff
            selected = avail.head(req)
            selected_ids = [str(eid) for eid in selected["id"].tolist()]
 
            # Aplicar asignaciones
            for emp_id in selected_ids:
                exec_sql("""
                    insert into shift_assignments (work_date, iso_dow, shift_type_id, employee_id, active)
                    values (:dt, :dow, :shift, :emp, true)
                    on conflict (work_date, shift_type_id, employee_id)
                    do update set active=true
                """, {"dt": str(d), "dow": iso_dow, "shift": shift_id, "emp": emp_id})
                # Actualizar contador en memoria
                shift_counts[emp_id] = shift_counts.get(emp_id, 0) + 1
 
            # Estadísticas
            if len(selected_ids) >= req:
                stats["cubiertos"] += 1
            else:
                stats["parciales"] += 1
 
        d += timedelta(days=1)
 
    # Invalidar caché una sola vez al terminar todo el proceso de escritura
    _invalidate_caches()
    return stats
 
 
# ---------- UI ----------
 
st.markdown(
"<link href='https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:wght@300;400;500;600&display=swap' rel='stylesheet'>",
unsafe_allow_html=True)
 
st.markdown("""<style>
.stApp, .stApp * {
    font-family: 'DM Sans', sans-serif !important;
}
 
.stApp iframe, .stApp iframe * {
    font-family: inherit;
}
 
.stApp {
    background-color: #F7F6F2 !important;
}
section[data-testid="stSidebar"] {
    background-color: #F0EFE9 !important;
}
 
h1 {
    font-family: 'DM Serif Display', serif !important;
    font-size: 2.4rem !important;
    font-weight: 400 !important;
    color: #1C2B1E !important;
    letter-spacing: -0.02em !important;
    padding-bottom: 0.1rem !important;
}
 
h2, h3 {
    font-family: 'DM Serif Display', serif !important;
    font-weight: 400 !important;
    color: #1C2B1E !important;
    letter-spacing: -0.01em !important;
}
h2 { font-size: 1.5rem !important; }
h3 { font-size: 1.2rem !important; }
 
button[data-baseweb="tab"] {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase !important;
    color: #7A8C7C !important;
    border-radius: 0 !important;
    padding: 0.6rem 1.2rem !important;
    border-bottom: 2px solid transparent !important;
    transition: all 0.2s ease !important;
}
button[data-baseweb="tab"]:hover {
    color: #2D5A35 !important;
    background: transparent !important;
}
button[aria-selected="true"][data-baseweb="tab"] {
    color: #2D5A35 !important;
    border-bottom: 2px solid #2D5A35 !important;
    background: transparent !important;
}
 
button[kind="primary"], .stButton > button[kind="primary"] {
    background-color: #2D5A35 !important;
    color: #F7F6F2 !important;
    border: none !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.875rem !important;
    letter-spacing: 0.02em !important;
    padding: 0.5rem 1.4rem !important;
    transition: background 0.2s ease !important;
}
button[kind="primary"]:hover {
    background-color: #1C3D22 !important;
}
 
.stButton > button {
    background-color: #ECEAE3 !important;
    color: #1C2B1E !important;
    border: 1px solid #D4D1C7 !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 400 !important;
    font-size: 0.875rem !important;
    transition: all 0.2s ease !important;
}
.stButton > button:hover {
    background-color: #DDD9CF !important;
    border-color: #B8B4A8 !important;
}
 
input[type="text"], input[type="number"],
.stTextInput > div > div > input,
.stNumberInput > div > div > input {
    background-color: #FFFFFF !important;
    border: 1px solid #D4D1C7 !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.9rem !important;
    color: #1C2B1E !important;
    transition: border 0.15s ease !important;
}
input:focus {
    border-color: #2D5A35 !important;
    box-shadow: 0 0 0 2px rgba(45,90,53,0.12) !important;
    outline: none !important;
}
 
.stSelectbox > div > div,
.stMultiSelect > div > div {
    background-color: #FFFFFF !important;
    border: 1px solid #D4D1C7 !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.9rem !important;
}
 
.stDateInput > div > div > input {
    background-color: #FFFFFF !important;
    border: 1px solid #D4D1C7 !important;
    border-radius: 6px !important;
    font-family: 'DM Sans', sans-serif !important;
}
 
.stDataFrame {
    border: 1px solid #E2DFD8 !important;
    border-radius: 8px !important;
    overflow: hidden !important;
}
.stDataFrame thead tr th {
    background-color: #ECEAE3 !important;
    color: #4A5C4C !important;
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    border-bottom: 1px solid #D4D1C7 !important;
    padding: 10px 14px !important;
}
.stDataFrame tbody tr td {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
    color: #2C3E2E !important;
    padding: 9px 14px !important;
    border-bottom: 1px solid #F0EFE9 !important;
}
.stDataFrame tbody tr:hover td {
    background-color: #F0EFE9 !important;
}
 
[data-testid="metric-container"] {
    background-color: #FFFFFF !important;
    border: 1px solid #E2DFD8 !important;
    border-radius: 10px !important;
    padding: 1rem 1.2rem !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04) !important;
}
[data-testid="metric-container"] label {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.75rem !important;
    font-weight: 500 !important;
    letter-spacing: 0.06em !important;
    text-transform: uppercase !important;
    color: #7A8C7C !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-family: 'DM Serif Display', serif !important;
    font-size: 2rem !important;
    color: #1C2B1E !important;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"] {
    font-size: 0.78rem !important;
    font-weight: 500 !important;
    color: #2D5A35 !important;
}
 
.stSuccess > div {
    background-color: #EAF2EB !important;
    border-left: 3px solid #2D5A35 !important;
    border-radius: 6px !important;
    color: #1C3D22 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
}
.stWarning > div {
    background-color: #FDF6E8 !important;
    border-left: 3px solid #C8941A !important;
    border-radius: 6px !important;
    color: #7A5A10 !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
}
.stError > div {
    background-color: #FCECEA !important;
    border-left: 3px solid #C0392B !important;
    border-radius: 6px !important;
    color: #7A1F1A !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
}
.stInfo > div {
    background-color: #EEF3F8 !important;
    border-left: 3px solid #3B7ABF !important;
    border-radius: 6px !important;
    color: #1E3D5C !important;
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
}
 
.streamlit-expanderHeader {
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
    color: #4A5C4C !important;
    background-color: #F0EFE9 !important;
    border-radius: 6px !important;
}
 
.stCheckbox label {
    font-family: 'DM Sans', sans-serif !important;
    font-size: 0.88rem !important;
    color: #2C3E2E !important;
}
 
.stCaption, small, .stMarkdown p small {
    font-family: 'DM Sans', sans-serif !important;
    color: #8A9E8C !important;
    font-size: 0.8rem !important;
}
 
hr {
    border-color: #E2DFD8 !important;
    margin: 1.2rem 0 !important;
}
 
[data-testid="stForm"] {
    background-color: #FFFFFF !important;
    border: 1px solid #E2DFD8 !important;
    border-radius: 10px !important;
    padding: 1.2rem !important;
}
 
.stSpinner > div {
    border-top-color: #2D5A35 !important;
}
 
::-webkit-scrollbar { width: 6px; height: 6px; }
::-webkit-scrollbar-track { background: #F0EFE9; }
::-webkit-scrollbar-thumb { background: #C8C4BA; border-radius: 3px; }
::-webkit-scrollbar-thumb:hover { background: #A8A49A; }
 
iframe[title="streamlit_calendar.streamlit_calendar"] {
    min-height: 800px !important;
    width: 100% !important;
    border: none !important;
    background: transparent !important;
}
 
[data-testid="stIFrame"] {
    min-height: 800px !important;
    width: 100% !important;
}
</style>
""", unsafe_allow_html=True)
 
st.markdown("""
<div style="display:flex; align-items:baseline; gap:12px; margin-bottom:0.2rem;">
  <span style="font-family:'DM Serif Display',serif; font-size:2.4rem; color:#1C2B1E; letter-spacing:-0.02em; font-weight:400;">
    Turnos Farmacia
  </span>
  <span style="font-family:'DM Sans',sans-serif; font-size:0.78rem; font-weight:500; letter-spacing:0.1em; text-transform:uppercase; color:#7A8C7C; padding-bottom:6px;">
    gestión de personal
  </span>
</div>
""", unsafe_allow_html=True)
 
tab1, tab2, tab3 = st.tabs(["Personas", "Calendario mensual", "Dashboard mensual"])
 
# ===================== TAB 1: PERSONAS =====================
with tab1:
    st.subheader("Equipo")
 
    colA, colB = st.columns([1, 2], gap="large")
 
    with colA:
        with st.form("add_person", clear_on_submit=True):
            name = st.text_input("Nombre")
            role = st.selectbox("Rol", ["empleada", "encargada"])
            ok = st.form_submit_button("➕ Añadir")
            if ok and name.strip():
                exec_sql("""
                    insert into employees (full_name, role, active)
                    values (:n, :r, true)
                """, {"n": name.strip(), "r": role})
                st.success("Persona creada.")
 
    df_all = read_df("select id, full_name, role, active from employees order by full_name")
    if df_all.empty:
        st.info("Aún no hay personas.")
    else:
        with colB:
            st.dataframe(df_all[["full_name","role","active"]], use_container_width=True, hide_index=True)
 
        st.divider()
        st.subheader("Editar / Desactivar persona")
        names = df_all["full_name"].tolist()
        sel = st.selectbox("Selecciona persona", names)
        sel_row = df_all[df_all["full_name"] == sel].iloc[0]
        sel_id = sel_row["id"]
 
        c1, c2, c3 = st.columns([2, 1, 1])
        with c1:
            new_name = st.text_input("Nombre (editar)", value=sel_row["full_name"])
        with c2:
            new_role = st.selectbox("Rol", ["empleada","encargada"], index=0 if sel_row["role"]=="empleada" else 1)
        with c3:
            new_active = st.checkbox("Activa", value=bool(sel_row["active"]))
 
        if st.button("💾 Guardar cambios"):
            exec_sql("""
                update employees
                set full_name=:n, role=:r, active=:a
                where id=:id
            """, {"n": new_name.strip(), "r": new_role, "a": new_active, "id": sel_id})
            st.success("Guardado. Recarga si no ves cambios.")
            st.rerun()
 
        st.divider()
        st.subheader("Disponibilidad semanal (día + turno)")
 
        shifts = get_active_shifts()
        if shifts.empty:
            st.warning("No hay turnos activos en shift_types.")
        else:
            for dow in range(1, 8):
                for _, sh in shifts.iterrows():
                    exec_sql("""
                        insert into employee_weekly_availability (employee_id, iso_dow, shift_type_id, available)
                        values (:e, :d, :s, true)
                        on conflict (employee_id, iso_dow, shift_type_id) do nothing
                    """, {"e": sel_id, "d": dow, "s": sh["id"]})
 
            current = read_df("""
                select iso_dow, shift_type_id, available
                from employee_weekly_availability
                where employee_id=:e
            """, {"e": sel_id})
 
            st.caption("Marca lo que normalmente puede hacer esta persona.")
 
            for dow in range(1, 8):
                row_cols = st.columns([1] + [2]*len(shifts))
                row_cols[0].write(f"**{ISO_DOW[dow]}**")
 
                for i, sh in enumerate(shifts.itertuples(index=False), start=1):
                    val = current[(current["iso_dow"] == dow) & (current["shift_type_id"] == sh.id)]
                    cur = bool(val.iloc[0]["available"]) if not val.empty else True
                    key = f"avail_{sel_id}_{dow}_{sh.id}"
                    new = row_cols[i].checkbox(f"{sh.name}", value=cur, key=key)
                    if new != cur:
                        upsert_weekly_availability(sel_id, dow, sh.id, new)
                        st.toast("Disponibilidad guardada ✅")
 
# ===================== TAB 2: CALENDARIO MENSUAL =====================
with tab2:
    st.subheader("Calendario mensual")
    st.caption("Pulsa en un bloque del calendario para editar ese turno en el panel de la derecha.")
 
    shifts = get_active_shifts()
    if shifts.empty:
        st.warning("No hay turnos activos en shift_types.")
        st.stop()
 
    pick = st.date_input("Mes", value=date.today(), key="cal_month")
    start, end = month_range(pick)
 
    # --- AUTOASIGNACION ---
    st.divider()
    with st.container():
        col_auto1, col_auto2, col_auto3 = st.columns([2, 1, 2])
 
        with col_auto1:
            st.markdown("#### 🤖 Autoasignación inteligente")
            st.caption(
                "Asigna automáticamente el mes completo eligiendo, para cada turno, "
                "las personas disponibles con menor carga acumulada en el mes."
            )
 
        with col_auto2:
            only_empty = st.checkbox(
                "Solo días vacíos",
                value=True,
                help="Si está marcado, solo asigna turnos que aún no tienen ninguna persona asignada. "
                     "Si lo desmarco, puede sobreescribir asignaciones existentes."
            )
 
        with col_auto3:
            st.write("")  # spacer
            run_auto = st.button(
                "🤖 Generar asignaciones del mes",
                type="primary",
                use_container_width=True,
                key="run_auto_assign"
            )
 
        if run_auto:
            with st.spinner(f"Generando asignaciones para {pick.strftime('%B %Y')}…"):
                result = auto_assign_month(pick, shifts, only_empty=only_empty)
 
            total_procesados = result["total"] - result["saltados"]
            st.success(
                f"✅ Autoasignación completada para **{pick.strftime('%B %Y')}**\n\n"
                f"- 🟢 Turnos cubiertos al 100%: **{result['cubiertos']}**\n"
                f"- 🟡 Turnos con cobertura parcial: **{result['parciales']}**\n"
                f"- 🔴 Turnos sin personal disponible: **{result['sin_personal']}**\n"
                f"- ⏭️ Turnos ya asignados (saltados): **{result['saltados']}**"
            )
            if result["parciales"] > 0 or result["sin_personal"] > 0:
                st.warning(
                    "Los turnos con cobertura parcial o sin personal aparecen en **rojo** en el calendario. "
                    "Puedes asignarlos manualmente pulsando sobre ellos."
                )
            st.rerun()
 
    st.divider()
    # ---
 
    col_cal, col_edit = st.columns([3, 2], gap="large")
 
    with col_cal:
        df_ass = read_df("""
            select a.work_date, a.shift_type_id, e.full_name
            from shift_assignments a
            join employees e on e.id=a.employee_id
            where a.active=true and a.work_date >= :s and a.work_date < :e
            order by a.work_date, a.shift_type_id, e.full_name
        """, {"s": str(start), "e": str(end)})
 
        assigned_map = {}
        if not df_ass.empty:
            for (wd, sid), g in df_ass.groupby(["work_date", "shift_type_id"]):
                assigned_map[(str(wd), str(sid))] = g["full_name"].tolist()
 
        # Build calendar data for HTML component
        cal_days = []
        d = start
        while d < end:
            iso = d.isoformat()
            day_shifts = []
            for sh in shifts.itertuples(index=False):
                names = assigned_map.get((iso, str(sh.id)), [])
                req = int(sh.required_staff)
                count = len(names)
                nm = str(sh.name).lower()
                short_code = "M" if "mañ" in nm else ("T" if "tar" in nm else str(sh.code))
                if count >= req:
                    color = "#27ae60"
                elif count == req - 1:
                    color = "#e67e22"
                else:
                    color = "#e74c3c"
                short_names = ", ".join(names[:2]) if names else "sin asignar"
                more = f" +{count-2}" if count > 2 else ""
                day_shifts.append({
                    "id": f"{iso}|{sh.id}",
                    "code": short_code,
                    "label": f"{short_names}{more}",
                    "color": color,
                })
            cal_days.append({
                "date": iso,
                "day": d.day,
                "dow": d.isoweekday(),
                "shifts": day_shifts,
            })
            d += timedelta(days=1)
 
        # First day of week offset (Monday=1)
        first_dow = start.isoweekday()  # 1=Mon..7=Sun
        month_name = start.strftime("%B %Y")
 
        cal_data_json = json.dumps(cal_days)
 
        cal_html = f"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: 'DM Sans', -apple-system, sans-serif; background: transparent; }}
.cal-header {{
    display: flex; align-items: center; justify-content: space-between;
    padding: 0 0 12px 0;
}}
.cal-title {{
    font-size: 1rem; font-weight: 600; color: #1C2B1E; letter-spacing: 0.02em;
}}
.cal-grid {{
    display: grid;
    grid-template-columns: repeat(7, 1fr);
    gap: 3px;
}}
.cal-dow {{
    text-align: center; font-size: 0.7rem; font-weight: 600;
    letter-spacing: 0.08em; text-transform: uppercase;
    color: #7A8C7C; padding: 4px 0 8px 0;
}}
.cal-dow.weekend {{ color: #b0a898; }}
.cal-cell {{
    background: #fff;
    border: 1px solid #E8E5DE;
    border-radius: 6px;
    padding: 5px;
    min-height: 80px;
    cursor: pointer;
    transition: border-color 0.15s, box-shadow 0.15s;
}}
.cal-cell:hover {{
    border-color: #2D5A35;
    box-shadow: 0 0 0 2px rgba(45,90,53,0.1);
}}
.cal-cell.empty {{
    background: transparent; border-color: transparent; cursor: default;
}}
.cal-cell.today {{ border-color: #2D5A35; border-width: 2px; }}
.day-num {{
    font-size: 0.75rem; font-weight: 600; color: #4A5C4C;
    margin-bottom: 4px; text-align: right;
}}
.cal-cell.today .day-num {{ color: #2D5A35; }}
.shift-pill {{
    border-radius: 4px;
    padding: 2px 5px;
    font-size: 0.65rem;
    font-weight: 500;
    color: #fff;
    margin-bottom: 2px;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    display: block;
}}
.cal-cell.selected {{
    border-color: #2D5A35 !important;
    border-width: 2px !important;
    box-shadow: 0 0 0 3px rgba(45,90,53,0.2) !important;
}}
</style>
</head>
<body>
<div class="cal-header">
  <span class="cal-title">{month_name}</span>
</div>
<div class="cal-grid" id="grid"></div>
<script>
const days = {cal_data_json};
const firstDow = {first_dow};
const today = new Date().toISOString().split('T')[0];
const grid = document.getElementById('grid');
 
const dowLabels = ['Lun','Mar','Mie','Jue','Vie','Sab','Dom'];
const isWeekend = [false,false,false,false,false,true,true];
dowLabels.forEach((d,i) => {{
  const el = document.createElement('div');
  el.className = 'cal-dow' + (isWeekend[i] ? ' weekend' : '');
  el.textContent = d;
  grid.appendChild(el);
}});
 
// Empty cells before first day
for (let i = 1; i < firstDow; i++) {{
  const el = document.createElement('div');
  el.className = 'cal-cell empty';
  grid.appendChild(el);
}}
 
days.forEach(day => {{
  const cell = document.createElement('div');
  cell.className = 'cal-cell' + (day.date === today ? ' today' : '');
  cell.dataset.date = day.date;
 
  const num = document.createElement('div');
  num.className = 'day-num';
  num.textContent = day.day;
  cell.appendChild(num);
 
  day.shifts.forEach(sh => {{
    const pill = document.createElement('span');
    pill.className = 'shift-pill';
    pill.style.background = sh.color;
    pill.textContent = sh.code + ': ' + sh.label;
    pill.dataset.id = sh.id;
    cell.appendChild(pill);
  }});
 
  cell.addEventListener('click', function(e) {{
    const pill = e.target.closest('.shift-pill');
    const id = pill ? pill.dataset.id : (day.shifts.length > 0 ? day.shifts[0].id : null);
    if (!id) return;
    document.querySelectorAll('.cal-cell').forEach(c => c.classList.remove('selected'));
    cell.classList.add('selected');
    window.parent.postMessage({{type: 'cal_click', id: id}}, '*');
  }});
 
  grid.appendChild(cell);
}});
</script>
</body>
</html>
"""
        components.html(cal_html, height=620, scrolling=False)
 
        # Handle calendar click via query params
        click_id = st.query_params.get("cal_click", None)
        if click_id:
            try:
                work_date_str, shift_id = click_id.split("|", 1)
                st.session_state["selected_work_date"] = work_date_str
                st.session_state["selected_shift_id"] = shift_id
                st.query_params.clear()
                st.rerun()
            except Exception:
                pass
 
        # Fallback: click via selectbox for reliability
        st.caption("O selecciona directamente:")
        all_options = []
        d2 = start
        while d2 < end:
            for sh in shifts.itertuples(index=False):
                nm = str(sh.name).lower()
                short_code = "M" if "mañ" in nm else ("T" if "tar" in nm else str(sh.code))
                label = f"{d2.strftime('%d/%m')} {short_code} — {sh.name}"
                all_options.append((label, d2.isoformat(), str(sh.id)))
            d2 += timedelta(days=1)
 
        sel_label = st.selectbox(
            "Día y turno",
            options=["(ninguno)"] + [o[0] for o in all_options],
            key="cal_selectbox"
        )
        if sel_label != "(ninguno)":
            chosen = next((o for o in all_options if o[0] == sel_label), None)
            if chosen:
                st.session_state["selected_work_date"] = chosen[1]
                st.session_state["selected_shift_id"] = chosen[2]
 
    with col_edit:
        st.markdown("### Editor del turno")
 
        if "selected_work_date" not in st.session_state or "selected_shift_id" not in st.session_state:
            st.info("Pulsa en un bloque del calendario (M/T) para editarlo aquí.")
        else:
            if st.button("❌ Cerrar editor", key="close_editor"):
                st.session_state.pop("selected_work_date", None)
                st.session_state.pop("selected_shift_id", None)
                st.rerun()
 
            work_date_str = st.session_state["selected_work_date"]
            shift_id = st.session_state["selected_shift_id"]
 
            work_date = date.fromisoformat(work_date_str)
            dow = int(work_date.isoweekday())
 
            match = shifts[shifts["id"].astype(str) == str(shift_id)]
            if match.empty:
                st.error("No pude identificar el turno seleccionado.")
            else:
                sh_row = match.iloc[0]
                req = int(sh_row["required_staff"])
 
                st.write(f"**Fecha:** {work_date_str}")
                st.write(f"**Turno:** {sh_row['name']} ({sh_row['start_time']}–{sh_row['end_time']})")
                st.write(f"**Necesarias:** {req}")
 
                avail = available_employees_for_date_shift(work_date, dow, str(shift_id))
                if avail.empty:
                    st.warning("Nadie disponible según disponibilidad/vacaciones.")
                else:
                    avail_names = avail["full_name"].tolist()
                    avail_map = dict(zip(avail_names, avail["id"].tolist()))
 
                    assigned = get_assignments(work_date, str(shift_id))
                    assigned_active = (
                        assigned[assigned["active"] == True]["full_name"].tolist()
                        if not assigned.empty
                        else []
                    )
 
                    selected = st.multiselect(
                        "Asignar personas (quedarán ACTIVAS)",
                        options=avail_names,
                        default=[n for n in assigned_active if n in avail_map],
                        key=f"ms_{work_date_str}_{shift_id}",
                    )
 
                    if st.button("💾 Guardar asignación", type="primary", key=f"save_{work_date_str}_{shift_id}"):
                        selected_ids = [avail_map[n] for n in selected]
                        apply_assignments(work_date, dow, str(shift_id), selected_ids)
                        st.success("Guardado.")
                        st.rerun()
 
                    st.divider()
                    st.caption("Asignaciones existentes (activar/desactivar):")
                    if assigned.empty:
                        st.info("No hay asignaciones todavía.")
                    else:
                        for r in assigned.itertuples(index=False):
                            k = f"act_{r.assignment_id}"
                            new_act = st.checkbox(r.full_name, value=bool(r.active), key=k)
                            if new_act != bool(r.active):
                                set_assignment_active(r.assignment_id, new_act)
                                st.toast("Actualizado ✅")
                                st.rerun()
 
                    st.divider()
                    with st.expander("🛠️ Disponibilidad puntual (override)", expanded=False):
                        st.caption("Solo este día y este turno.")
 
                        df_eff = get_effective_availability_all(work_date, dow, str(shift_id))
                        reason = st.text_input("Motivo (opcional)", value="", key=f"ov_reason_{work_date_str}_{shift_id}")
 
                        for r in df_eff.itertuples(index=False):
                            if r.is_time_off:
                                st.checkbox(
                                    f"{r.full_name} (vacaciones)",
                                    value=False,
                                    key=f"ov_{r.id}_{work_date_str}_{shift_id}",
                                    disabled=True
                                )
                                continue
 
                            new_av = st.checkbox(
                                r.full_name,
                                value=bool(r.is_available),
                                key=f"ov_{r.id}_{work_date_str}_{shift_id}"
                            )
                            if new_av != bool(r.is_available):
                                upsert_override(
                                    emp_id=str(r.id),
                                    work_date=work_date,
                                    shift_id=str(shift_id),
                                    available=new_av,
                                    reason=reason
                                )
                                st.toast("Override guardado ✅")
                                st.rerun()
 
# ===================== TAB 3: DASHBOARD =====================
with tab3:
    st.subheader("Dashboard")
    st.caption("Horas y cobertura según asignaciones activas en el rango seleccionado.")
 
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        dash_start = st.date_input("Inicio", value=date.today().replace(day=1), key="dash_start")
    with c2:
        dash_end = st.date_input("Fin", value=date.today(), key="dash_end")
    with c3:
        st.info("El rango incluye Inicio y Fin.")
 
    if dash_end < dash_start:
        st.error("La fecha 'Fin' no puede ser anterior a 'Inicio'.")
    else:
        # --- ALERTAS DE COBERTURA ---
        st.markdown("### 🚨 Alertas de cobertura")
        st.caption("Turnos con personal insuficiente respecto al mínimo requerido.")
 
        try:
            shifts_dash = get_active_shifts()
 
            if shifts_dash.empty:
                st.warning("No hay turnos activos configurados.")
            else:
                # Asignaciones activas del rango agrupadas por día+turno
                df_cov = read_df("""
                    select
                      a.work_date::text  as work_date,
                      a.shift_type_id::text as shift_type_id,
                      count(*)           as assigned
                    from shift_assignments a
                    where a.active = true
                      and a.work_date >= :s
                      and a.work_date <= :e
                    group by a.work_date, a.shift_type_id
                """, {"s": str(dash_start), "e": str(dash_end)})
 
                # Construir tabla completa días × turnos
                coverage_rows = []
                cur = dash_start
                while cur <= dash_end:
                    cur_str = str(cur)
                    for sh in shifts_dash.itertuples(index=False):
                        sh_id_str = str(sh.id)
                        assigned_count = 0
                        if not df_cov.empty:
                            mask = (
                                (df_cov["work_date"] == cur_str) &
                                (df_cov["shift_type_id"] == sh_id_str)
                            )
                            rows = df_cov[mask]
                            if not rows.empty:
                                assigned_count = int(rows.iloc[0]["assigned"])
                        req = int(sh.required_staff)
                        coverage_rows.append({
                            "fecha":      cur,
                            "turno":      sh.name,
                            "requeridas": req,
                            "asignadas":  assigned_count,
                            "deficit":    max(0, req - assigned_count),
                        })
                    cur += timedelta(days=1)
 
                df_cov_full = pd.DataFrame(coverage_rows)
                total_slots = len(df_cov_full)
 
                if total_slots == 0:
                    st.info("No hay datos de turnos para este rango.")
                else:
                    cubiertos = int((df_cov_full["deficit"] == 0).sum())
                    parciales = int(((df_cov_full["deficit"] > 0) & (df_cov_full["asignadas"] > 0)).sum())
                    vacios    = int((df_cov_full["asignadas"] == 0).sum())
                    pct_ok    = round(cubiertos / total_slots * 100)
 
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("✅ Cubiertos",       cubiertos, f"{pct_ok}%")
                    m2.metric("🟡 Parciales",        parciales)
                    m3.metric("🔴 Sin asignar",      vacios)
                    m4.metric("📋 Total turnos",     total_slots)
 
                    df_prob = df_cov_full[df_cov_full["deficit"] > 0].copy()
                    if df_prob.empty:
                        st.success("🎉 ¡Cobertura completa en todo el rango!")
                    else:
                        # Ordenar por fecha real antes de formatear a string
                        df_prob = df_prob.sort_values(["fecha", "turno"])
                        df_prob["estado"] = df_prob["asignadas"].apply(
                            lambda x: "🔴 Sin personal" if x == 0 else "🟡 Parcial"
                        )
                        df_prob["Fecha"] = pd.to_datetime(
                            df_prob["fecha"].apply(str), format="%Y-%m-%d"
                        ).dt.strftime("%d/%m/%Y (%a)")
 
                        st.dataframe(
                            df_prob[["Fecha","turno","requeridas","asignadas","deficit","estado"]]
                            .rename(columns={
                                "turno":      "Turno",
                                "requeridas": "Requeridas",
                                "asignadas":  "Asignadas",
                                "deficit":    "Déficit",
                                "estado":     "Estado",
                            }),
                            use_container_width=True,
                            hide_index=True,
                        )
 
                        st.markdown("#### Déficit acumulado por turno")
                        df_by_shift = (
                            df_prob.groupby("turno", as_index=False)
                            .agg(
                                dias_con_deficit=("deficit", "count"),
                                deficit_total=("deficit", "sum"),
                            )
                            .rename(columns={
                                "turno":            "Turno",
                                "dias_con_deficit": "Días con déficit",
                                "deficit_total":    "Personas faltantes (total)",
                            })
                            .sort_values("Personas faltantes (total)", ascending=False)
                        )
                        st.dataframe(df_by_shift, use_container_width=True, hide_index=True)
 
        except Exception as e:
            st.error(f"Error en alertas de cobertura: {e}")
 
        st.divider()
 
        # --- HORAS POR PERSONA ---
        try:
            df_h = read_df("""
                select
                  e.full_name,
                  st.name          as turno,
                  st.start_time::text as start_time,
                  st.end_time::text   as end_time,
                  a.work_date
                from shift_assignments a
                join employees  e  on e.id  = a.employee_id
                join shift_types st on st.id = a.shift_type_id
                where a.active = true
                  and a.work_date >= :s
                  and a.work_date <= :e
                order by e.full_name, a.work_date, st.start_time
            """, {"s": str(dash_start), "e": str(dash_end)})
 
            if df_h.empty:
                st.info("No hay asignaciones activas en ese rango.")
            else:
                # Calcular duración del turno de forma segura
                def parse_time(t):
                    """Acepta 'HH:MM:SS', 'HH:MM:SS.ffffff', 'HH:MM' o timedelta de psycopg2."""
                    if isinstance(t, str):
                        parts = t.split(":")
                        return int(parts[0]) * 3600 + int(parts[1]) * 60 + (int(float(parts[2])) if len(parts) > 2 else 0)
                    try:
                        return int(t.total_seconds())
                    except Exception:
                        return 0
 
                df_h["_s"] = df_h["start_time"].apply(parse_time)
                df_h["_e"] = df_h["end_time"].apply(parse_time)
                df_h["hours"] = (df_h["_e"] - df_h["_s"]) / 3600.0
 
                resumen = (
                    df_h.groupby("full_name", as_index=False)
                    .agg(turnos=("turno", "count"), horas=("hours", "sum"))
                    .sort_values("horas", ascending=False)
                )
 
                st.markdown("### Horas por persona")
                st.dataframe(resumen, use_container_width=True, hide_index=True)
 
                st.markdown("### Detalle")
                st.dataframe(
                    df_h[["work_date", "turno", "full_name", "hours"]],
                    use_container_width=True,
                    hide_index=True,
                )
 
        except Exception as e:
            st.error(f"Error calculando horas: {e}")
 
