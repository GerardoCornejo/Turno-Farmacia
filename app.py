import streamlit as st
import pandas as pd
from streamlit_calendar import calendar
from datetime import date, timedelta
from sqlalchemy import create_engine, text
 
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
    st.subheader("Calendario mensual (vista tipo Outlook)")
    st.caption("En cada día verás 2 bloques (M/T). Pulsa en un bloque para editarlo en el panel de la derecha.")
 
    shifts = get_active_shifts()
    if shifts.empty:
        st.warning("No hay turnos activos en shift_types.")
        st.stop()
 
    pick = st.date_input("Mes", value=date.today(), key="cal_month")
    start, end = month_range(pick)
 
    # ── AUTOASIGNACIÓN ──────────────────────────────────────────────
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
    # ────────────────────────────────────────────────────────────────
 
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
 
        events = []
        d = start
        while d < end:
            iso = d.isoformat()
            for sh in shifts.itertuples(index=False):
                names = assigned_map.get((iso, str(sh.id)), [])
 
                icon = "✎" if names else "+"
                nm = str(sh.name).lower()
                short_code = "M" if "mañ" in nm else ("T" if "tar" in nm else str(sh.code))
 
                short_names = ", ".join(names[:2]) if names else "—"
                more = f" +{len(names)-2}" if len(names) > 2 else ""
                title = f"{short_code}: {short_names}{more}  {icon}"
 
                req = int(sh.required_staff)
                count = len(names)
                if count >= req:
                    color = "#2ecc71"
                elif count == req - 1:
                    color = "#f1c40f"
                else:
                    color = "#e74c3c"
 
                events.append({
                    "id": f"{iso}|{sh.id}",
                    "title": title,
                    "start": iso,
                    "allDay": True,
                    "backgroundColor": color,
                    "borderColor": color,
                })
            d += timedelta(days=1)
 
        options = {
            "initialView": "dayGridMonth",
            "headerToolbar": {"left": "prev,next today", "center": "title", "right": "dayGridMonth,listMonth"},
            "height": 780,
            "firstDay": 1,
            "dayMaxEvents": True,
        }
 
        cal_state = calendar(events=events, options=options, key="fullcalendar")
 
        clicked = None
        if isinstance(cal_state, dict):
            clicked = cal_state.get("eventClick")
 
        if clicked and "event" in clicked and "id" in clicked["event"]:
            event_id = clicked["event"]["id"]
            try:
                work_date_str, shift_id = event_id.split("|", 1)
                st.session_state["selected_work_date"] = work_date_str
                st.session_state["selected_shift_id"] = shift_id
            except Exception:
                st.session_state.pop("selected_work_date", None)
                st.session_state.pop("selected_shift_id", None)
 
    with col_edit:
        st.markdown("### Editor del turno")
 
        if "selected_work_date" not in st.session_state or "selected_shift_id" not in st.session_state:
            st.info("Pulsa en un bloque del calendario (M/T) para editarlo aquí.")
            st.stop()
 
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
            st.stop()
 
        sh_row = match.iloc[0]
        req = int(sh_row["required_staff"])
 
        st.write(f"**Fecha:** {work_date_str}")
        st.write(f"**Turno:** {sh_row['name']} ({sh_row['start_time']}–{sh_row['end_time']})")
        st.write(f"**Necesarias:** {req}")
 
        avail = available_employees_for_date_shift(work_date, dow, str(shift_id))
        if avail.empty:
            st.warning("Nadie disponible según disponibilidad/vacaciones.")
            st.stop()
 
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
    st.subheader("Dashboard (horas reales por persona)")
    st.caption("Cuenta horas según asignaciones ACTIVAS en el rango de fechas elegido.")
 
    c1, c2, c3 = st.columns([1, 1, 2])
    with c1:
        start_date = st.date_input("Inicio", value=date.today().replace(day=1), key="dash_start")
    with c2:
        end_date = st.date_input("Fin", value=date.today(), key="dash_end")
    with c3:
        st.info("El rango incluye Inicio y Fin.")
 
    if end_date < start_date:
        st.error("La fecha 'Fin' no puede ser anterior a 'Inicio'.")
        st.stop()
 
    # ── ALERTAS DE COBERTURA ─────────────────────────────────────────
    st.markdown("### 🚨 Alertas de cobertura")
    st.caption("Turnos con personal insuficiente respecto al mínimo requerido.")
 
    shifts_dash = get_active_shifts()
 
    # Obtener asignaciones activas del rango agrupadas por día+turno
    df_cov = read_df("""
        select
          a.work_date,
          a.shift_type_id,
          count(*) as assigned
        from shift_assignments a
        where a.active=true
          and a.work_date >= :s
          and a.work_date <= :e
        group by a.work_date, a.shift_type_id
    """, {"s": str(start_date), "e": str(end_date)})
 
    # Construir tabla completa de días × turnos con cobertura esperada
    coverage_rows = []
    d = start_date
    while d <= end_date:
        for sh in shifts_dash.itertuples(index=False):
            req = int(sh.required_staff)
            assigned = 0
            if not df_cov.empty:
                match = df_cov[
                    (df_cov["work_date"].astype(str) == str(d)) &
                    (df_cov["shift_type_id"].astype(str) == str(sh.id))
                ]
                if not match.empty:
                    assigned = int(match.iloc[0]["assigned"])
            coverage_rows.append({
                "fecha": d,
                "turno": sh.name,
                "requeridas": req,
                "asignadas": assigned,
                "deficit": max(0, req - assigned),
            })
        d += timedelta(days=1)
 
    df_coverage = pd.DataFrame(coverage_rows)
 
    # Métricas resumen
    total_slots = len(df_coverage)
    cubiertos = int((df_coverage["deficit"] == 0).sum())
    parciales = int(((df_coverage["deficit"] > 0) & (df_coverage["asignadas"] > 0)).sum())
    vacios = int((df_coverage["asignadas"] == 0).sum())
 
    pct_ok = round(cubiertos / total_slots * 100) if total_slots > 0 else 0
 
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("✅ Turnos cubiertos", cubiertos, f"{pct_ok}% del total")
    m2.metric("🟡 Cobertura parcial", parciales)
    m3.metric("🔴 Sin asignar", vacios)
    m4.metric("📋 Total turnos", total_slots)
 
    # Tabla de días problemáticos
    df_problemas = df_coverage[df_coverage["deficit"] > 0].copy()
 
    if df_problemas.empty:
        st.success("🎉 ¡Cobertura completa en todo el rango seleccionado!")
    else:
        # Guardar fecha como date para ordenar correctamente ANTES de formatear a string
        df_problemas["fecha_ord"] = pd.to_datetime(
            df_problemas["fecha"].apply(str), format="%Y-%m-%d"
        )
        df_problemas["estado"] = df_problemas.apply(
            lambda r: "🔴 Sin personal" if r["asignadas"] == 0 else "🟡 Parcial", axis=1
        )
        # Formatear para mostrar DESPUÉS de ordenar
        df_problemas = df_problemas.sort_values(["fecha_ord", "turno"])
        df_problemas["fecha"] = df_problemas["fecha_ord"].dt.strftime("%d/%m/%Y (%a)")
 
        st.dataframe(
            df_problemas[["fecha", "turno", "requeridas", "asignadas", "deficit", "estado"]]
            .rename(columns={
                "fecha": "Fecha",
                "turno": "Turno",
                "requeridas": "Requeridas",
                "asignadas": "Asignadas",
                "deficit": "Déficit",
                "estado": "Estado",
            }),
            use_container_width=True,
            hide_index=True,
        )
 
        # Resumen de déficit por turno (sobre df_problemas antes del rename)
        st.markdown("#### Déficit acumulado por turno")
        df_by_shift = (
            df_problemas.groupby("turno")
            .agg(dias_con_deficit=("deficit", "count"), deficit_total=("deficit", "sum"))
            .reset_index()
            .rename(columns={
                "turno": "Turno",
                "dias_con_deficit": "Días con déficit",
                "deficit_total": "Personas faltantes (total)"
            })
            .sort_values("Personas faltantes (total)", ascending=False)
        )
        st.dataframe(df_by_shift, use_container_width=True, hide_index=True)
 
    st.divider()
    # ────────────────────────────────────────────────────────────────
 
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
          and a.work_date >= :s
          and a.work_date <= :e
        order by e.full_name, a.work_date, st.start_time
    """, {"s": str(start_date), "e": str(end_date)})
 
    if df.empty:
        st.info("No hay asignaciones activas en ese rango.")
    else:
        # Especificar formato explícito para evitar warnings de pandas
        df["start_time"] = pd.to_datetime(df["start_time"].astype(str), format="mixed")
        df["end_time"]   = pd.to_datetime(df["end_time"].astype(str), format="mixed")
        df["hours"] = (df["end_time"] - df["start_time"]).dt.total_seconds() / 3600.0
 
        resumen = df.groupby("full_name").agg(
            turnos=("turno", "count"),
            horas=("hours", "sum")
        ).reset_index().sort_values("horas", ascending=False)
 
        st.markdown("### Horas por persona (solo asignaciones activas)")
        st.dataframe(resumen, use_container_width=True, hide_index=True)
 
        st.markdown("### Detalle")
        st.dataframe(
            df[["work_date", "turno", "full_name", "hours"]],
            use_container_width=True,
            hide_index=True,
        )
