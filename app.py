import streamlit as st
import pandas as pd
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

# ===================== TAB 2: CALENDARIO MENSUAL =====================
with tab2:
    st.subheader("Calendario mensual (asignar y activar/desactivar)")
    st.caption("Aquí haces el calendario real. Se basa en la disponibilidad semanal + vacaciones. Puedes ajustar asignaciones y activarlas/desactivarlas.")

    shifts = get_active_shifts()
    if shifts.empty:
        st.warning("No hay turnos activos en shift_types.")
        st.stop()

    pick = st.date_input("Elige un día del mes", value=date.today())
    start, end = month_range(pick)

    # selector: mostrar solo turnos con falta de gente
    show_only_missing = st.checkbox("Mostrar solo turnos con falta de cobertura", value=False)

    d = start
    while d < end:
        dow = int(d.isoweekday())
        day_label = f"{d.strftime('%d/%m/%Y')} ({ISO_DOW[dow]})"
        st.markdown(f"### {day_label}")

        for sh in shifts.itertuples(index=False):
            req = int(sh.required_staff)
            avail = available_employees_for_date_shift(d, dow, sh.id)
            avail_names = avail["full_name"].tolist()
            avail_map = dict(zip(avail_names, avail["id"].tolist()))

            assigned = get_assignments(d, sh.id)
            assigned_active = assigned[assigned["active"] == True]["full_name"].tolist() if not assigned.empty else []
            assigned_all = assigned["full_name"].tolist() if not assigned.empty else []

            missing = max(0, req - len(assigned_active))
            if show_only_missing and missing == 0:
                continue

            with st.expander(f"{sh.name} ({sh.start_time}–{sh.end_time}) · Necesarias: {req} · Activas: {len(assigned_active)} · Falta: {missing}", expanded=not show_only_missing):
                if avail.empty:
                    st.warning("Nadie disponible (según disponibilidad semanal/vacaciones).")
                else:
                    # selección de asignaciones activas
                    default = [n for n in assigned_active if n in avail_map]  # por si alguien fue desactivado o ya no es disponible
                    selected = st.multiselect(
                    "Asignar personas (esto deja esas asignaciones ACTIVAS)",
                    options=avail_names,
                    default=default,
                    key=f"ms_{d.isoformat()}_{sh.id}"
                    )

                    if st.button(f"💾 Guardar asignación ({d} - {sh.name})", key=f"save_{d}_{sh.id}"):
                        selected_ids = [avail_map[n] for n in selected]
                        apply_assignments(d, dow, sh.id, selected_ids)
                        st.success("Asignación guardada.")
                        st.rerun()

                st.divider()
                st.write("Asignaciones existentes (puedes activar/desactivar una por una):")
                if assigned.empty:
                    st.info("No hay asignaciones todavía.")
                else:
                    for r in assigned.itertuples(index=False):
                        key = f"act_{r.assignment_id}"
                        new_act = st.checkbox(r.full_name, value=bool(r.active), key=key)
                        if new_act != bool(r.active):
                            set_assignment_active(r.assignment_id, new_act)
                            st.toast("Actualizado ✅")
                            st.rerun()

        d += timedelta(days=1)

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

