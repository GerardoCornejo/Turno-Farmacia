import streamlit as st
import pandas as pd
from streamlit_calendar import calendar
from datetime import date, timedelta
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Turnos Farmacia", layout="wide")

# ---------- DB ----------
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

ISO_DOW = {1:"Lun",2:"Mar",3:"Mié",4:"Jue",5:"Vie",6:"Sáb",7:"Dom"}

def upsert_override(emp_id: str, work_date: date, shift_id: str, available: bool, reason: str = ""):
    exec_sql("""
        insert into employee_availability_overrides (employee_id, work_date, shift_type_id, available, reason)
        values (:e, :dt, :s, :a, :r)
        on conflict (employee_id, work_date, shift_type_id)
        do update set available = excluded.available,
                      reason = excluded.reason
    """, {"e": emp_id, "dt": str(work_date), "s": shift_id, "a": available, "r": reason})

def get_effective_availability_all(work_date: date, iso_dow: int, shift_id: str):
    # Devuelve todas las empleadas activas con disponibilidad efectiva (weekly + override) y si está de vacaciones
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

def month_range(any_day_in_month: date):
    start = any_day_in_month.replace(day=1)
    if start.month == 12:
        end = date(start.year + 1, 1, 1)
    else:
        end = date(start.year, start.month + 1, 1)
    return start, end

def get_active_shifts():
    # Mañana / Tarde (o los que tengas activos)
    return read_df("""
        select id, code, name, start_time, end_time, required_staff
        from shift_types
        where active=true
        order by start_time
    """)

def get_active_employees():
    return read_df("""
        select id, full_name, role
        from employees
        where active=true
        order by full_name
    """)

def upsert_weekly_availability(emp_id, iso_dow, shift_id, available):
    exec_sql("""
        insert into employee_weekly_availability (employee_id, iso_dow, shift_type_id, available)
        values (:e, :d, :s, :a)
        on conflict (employee_id, iso_dow, shift_type_id)
        do update set available = excluded.available
    """, {"e": emp_id, "d": iso_dow, "s": shift_id, "a": available})

def available_employees_for_date_shift(work_date: date, iso_dow: int, shift_id: str):
    # disponibilidad efectiva: override > weekly > true, y excluye vacaciones
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

def set_assignment_active(assignment_id: str, active: bool):
    exec_sql("""
        update shift_assignments
        set active=:a
        where id=:id
    """, {"a": active, "id": assignment_id})

def month_start(d: date) -> date:
    return d.replace(day=1)

def next_month_start(d: date) -> date:
    ms = month_start(d)
    if ms.month == 12:
        return date(ms.year + 1, 1, 1)
    return date(ms.year, ms.month + 1, 1)

def is_month_closed(ms: date) -> bool:
    df = read_df("select month_start from month_closures where month_start=:m", {"m": str(ms)})
    return not df.empty

def close_month(ms: date, closed_by: str = ""):
    exec_sql("""
        insert into month_closures (month_start, closed_by)
        values (:m, :by)
        on conflict (month_start) do nothing
    """, {"m": str(ms), "by": closed_by})

def apply_assignments(work_date: date, iso_dow: int, shift_id: str, selected_employee_ids: list[str]):
    # Regla: NO borramos, solo activamos/desactivamos
    existing = read_df("""
        select employee_id, id as assignment_id
        from shift_assignments
        where work_date=:dt and shift_type_id=:shift
    """, {"dt": str(work_date), "shift": shift_id})

    existing_ids = set(existing["employee_id"].tolist()) if not existing.empty else set()
    selected_ids = set(selected_employee_ids)

    # Activar/crear los seleccionados
    for emp_id in selected_ids:
        exec_sql("""
            insert into shift_assignments (work_date, iso_dow, shift_type_id, employee_id, active)
            values (:dt, :dow, :shift, :emp, true)
            on conflict (work_date, shift_type_id, employee_id)
            do update set active=true
        """, {"dt": str(work_date), "dow": iso_dow, "shift": shift_id, "emp": emp_id})

    # Desactivar los que estaban y ya no están seleccionados
    to_deactivate = list(existing_ids - selected_ids)
    if to_deactivate:
        exec_sql("""
            update shift_assignments
            set active=false
            where work_date=:dt and shift_type_id=:shift and employee_id = any(:arr)
        """, {"dt": str(work_date), "shift": shift_id, "arr": to_deactivate})

# ---------- UI ----------
st.title("🧾 Turnos Farmacia")

tab1, tab2, tab3 = st.tabs(["👥 Personas", "🗓️ Calendario mensual", "📊 Dashboard mensual"])

# ===================== TAB 1: PERSONAS =====================
with tab1:
    st.subheader("Personas (crear, editar, desactivar)")

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

    # Lista + desactivar
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
            # asegurar filas base (por defecto true si no existe)
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

            # UI tipo tabla (7 filas x N turnos)
            st.caption("Marca lo que normalmente puede hacer esta persona. (Esto se usa como base para el calendario mensual).")

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

# ===================== TAB 2: CALENDARIO MENSUAL (FULLCALENDAR) =====================
with tab2:
    st.subheader("Calendario mensual (vista tipo Outlook)")
    st.caption("Haz clic en Mañana/Tarde de un día para asignar personas.")

    shifts = get_active_shifts()
    if shifts.empty:
        st.warning("No hay turnos activos en shift_types.")
        st.stop()

    pick = st.date_input("Mes", value=date.today())
    start, end = month_range(pick)

    # --- Crear eventos (2 por día: Mañana y Tarde) ---
    # Mapa de asignaciones activas por (fecha, shift_id) -> lista nombres
    df_ass = read_df("""
        select a.work_date, a.shift_type_id, e.full_name
        from shift_assignments a
        join employees e on e.id=a.employee_id
        where a.active=true and a.work_date >= :s and a.work_date < :e
        order by a.work_date, a.shift_type_id, e.full_name
    """, {"s": str(start), "e": str(end)})

    assigned_map = {}
    if not df_ass.empty:
        for (wd, sid), g in df_ass.groupby(["work_date","shift_type_id"]):
            assigned_map[(str(wd), str(sid))] = g["full_name"].tolist()

    # Eventos “clicables” por día/turno
    events = []
    d = start
    while d < end:
        iso = d.isoformat()
        dow = int(d.isoweekday())
        for sh in shifts.itertuples(index=False):
            names = assigned_map.get((iso, sh.id), [])
            # etiqueta corta para el turno
            short_code = "M" if "mañ" in sh.name.lower() else ("T" if "tar" in sh.name.lower() else sh.code)
            full_text = f"{sh.name}: " + (", ".join(names) if names else "—")
            # título corto para que no se amontone en el calendario
            short_names = ", ".join(names[:2]) if names else "—"
            more = f" +{len(names)-2}" if len(names) > 2 else ""
            title = f"{short_code}: {short_names}{more}"
            events.append({
                "id": f"{iso}|{sh.id}",
                "title": title,
                "start": iso,
                "allDay": True,
                "extendedProps": {"tooltip": full_text},  # <-- tooltip completo
})
        d += timedelta(days=1)

    # --- Opciones FullCalendar (mes a pantalla completa) ---
    options = {
        "initialView": "dayGridMonth",
        "headerToolbar": {
            "left": "prev,next today",
            "center": "title",
            "right": "dayGridMonth,listMonth"
        },
        "height": 750,  # “pantalla completa” aproximada
        "firstDay": 1,  # lunes
        "dayMaxEvents": True,
    }
   
    # Render calendario
    cal_state = calendar(events=events, options=options, key="fullcalendar")

    # --- Capturar click en evento (Mañana/Tarde de un día) ---
    clicked = None
    if isinstance(cal_state, dict):
        clicked = cal_state.get("eventClick")

    if clicked and "event" in clicked and "id" in clicked["event"]:
        event_id = clicked["event"]["id"]  # "YYYY-MM-DD|shift_uuid"
        work_date_str, shift_id = event_id.split("|", 1)
        st.session_state["selected_work_date"] = work_date_str
        st.session_state["selected_shift_id"] = shift_id

    # PANEL: solo si hay selección
if "selected_work_date" in st.session_state and "selected_shift_id" in st.session_state:
    work_date_str = st.session_state["selected_work_date"]
    shift_id = st.session_state["selected_shift_id"]

    work_date = date.fromisoformat(work_date_str)
    dow = int(work_date.isoweekday())

    match = shifts[shifts["id"].astype(str) == str(shift_id)]
    if match.empty:
        st.error("No he podido identificar el turno (shift_id) recibido del calendario.")
        st.write("shift_id recibido:", shift_id)
        st.write("Turnos disponibles (id, name, code):")
        st.dataframe(shifts[["id", "name", "code"]], use_container_width=True, hide_index=True)
        st.stop()

    sh_row = match.iloc[0]
    req = int(sh_row["required_staff"])

    st.divider()
    st.markdown(f"### Editar {work_date_str} · **{sh_row['name']}** (necesarias: {req})")

    # Disponibles según semanal + overrides + vacaciones
    avail = available_employees_for_date_shift(work_date, dow, str(shift_id))
    if avail.empty:
        st.warning("Nadie disponible según disponibilidad/vacaciones.")
        st.stop()

    avail_names = avail["full_name"].tolist()
    avail_map = dict(zip(avail_names, avail["id"].tolist()))

    assigned = get_assignments(work_date, str(shift_id))
    assigned_active = assigned[assigned["active"] == True]["full_name"].tolist() if not assigned.empty else []

    selected = st.multiselect(
        "Personas asignadas (quedarán ACTIVAS)",
        options=avail_names,
        default=[n for n in assigned_active if n in avail_map],
        key=f"ms_{work_date_str}_{shift_id}"
    )

    c1, c2 = st.columns([1, 2])
    with c1:
        if st.button("💾 Guardar asignación", type="primary", key=f"save_{work_date_str}_{shift_id}"):
            selected_ids = [avail_map[n] for n in selected]
            apply_assignments(work_date, dow, str(shift_id), selected_ids)
            st.success("Guardado.")
            st.rerun()
    with c2:
        st.caption("Puedes activar/desactivar asignaciones una a una más abajo.")

    st.divider()
    st.write("Asignaciones existentes (activar/desactivar):")
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

    # Overrides (disponibilidad puntual)
    with st.expander("🛠️ Editar disponibilidad SOLO para este día (override)", expanded=False):
        st.caption("Esto NO cambia la disponibilidad semanal. Solo afecta a este día y este turno.")

        df_eff = get_effective_availability_all(work_date, dow, str(shift_id))

        reason = st.text_input(
            "Motivo (opcional)",
            value="",
            key=f"ov_reason_{work_date_str}_{shift_id}"
        )

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

else:
    st.info("Haz clic en un bloque (Mañana/Tarde) del calendario para editarlo.")
# ===================== TAB 3: DASHBOARD =====================
with tab3:
    st.subheader("Dashboard mensual (horas reales por persona)")
    st.caption("Cuenta horas según asignaciones ACTIVAS del calendario.")

    pick = st.date_input("Mes a analizar", value=date.today(), key="dash_month")
    start, end = month_range(pick)

    df = read_df("""
        select
          e.full_name,
          st.name as turno,
          st.start_time,
          st.end_time,
          a.work_date
        from shift_assignments a
        join employees e on e.id = a.employee_id
        join shift_types st on st.id = a.shift_type_id
        where a.active=true
          and a.work_date >= :s and a.work_date < :e
        order by e.full_name, a.work_date, st.start_time
    """, {"s": str(start), "e": str(end)})

    if df.empty:
        st.info("No hay asignaciones activas en ese mes.")
        st.stop()

    # calcular horas del turno
    # (end_time - start_time) en horas. Asumimos turnos dentro del día (no nocturnos).
    df["start_time"] = pd.to_datetime(df["start_time"].astype(str))
    df["end_time"] = pd.to_datetime(df["end_time"].astype(str))
    df["hours"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 3600.0

    resumen = df.groupby("full_name").agg(
        turnos=("turno", "count"),
        horas=("hours", "sum")
    ).reset_index().sort_values("horas", ascending=False)

    st.markdown("### Horas por persona (solo asignaciones activas)")
    st.dataframe(resumen, use_container_width=True, hide_index=True)

    st.markdown("### Detalle")
    st.dataframe(df[["work_date","turno","full_name","hours"]], use_container_width=True, hide_index=True)












