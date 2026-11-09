"""F. Application -- BankDW Phase 2 (Berka). One app, three tabs (group sheet, 2026-10-18):

  Tổng quan  U01 Quản lý kinh doanh    BR01 dòng tiền theo kỳ và theo hạng
  Phân tích  U02 Chuyên viên phân tích BR02 tra cứu tài khoản · BR03 cảnh báo · BR04 xử lý cảnh báo
  Vận hành   U03 Kỹ sư dữ liệu         BR05 kiểm soát dữ liệu · BR06 giám sát pipeline ·
                                        BR07 sao lưu/khôi phục · BR08 báo cáo lịch sử (SCD2) ·
                                        chạy kịch bản KB (N01-N05)
Reads D (ClickHouse) for reports and E (PostgreSQL) for operations; sends commands to C through
the runner (http://spark:8000). No login (sheet: chưa phân quyền).
"""
import datetime as dt
import json
import os
import time
import urllib.request

import altair as alt
import clickhouse_connect
import pandas as pd
import psycopg2
import streamlit as st

st.set_page_config(page_title="BankDW", page_icon="🏦", layout="wide")
RUNNER = os.environ.get("RUNNER_URL", "http://flink:8000")
TIERS = ["Standard", "Silver", "Gold", "Platinum"]
STATUS_VI = {"NEW": "Chưa xem", "IN_REVIEW": "Đang xem xét", "RESOLVED": "Đã xử lý"}
RULE_VI = {"R1": "R1 · Tiền ra tăng mạnh", "R2": "R2 · Tài khoản ngủ đông hoạt động lại"}
KB_INFO = {
    "KB03": "Nạp giao dịch gian lận chuẩn bị sẵn → sinh cảnh báo (BR03)",
    "KB05": "Nạp lô có bản ghi lỗi chủ đích → tách lỗi, hiện số lỗi và lý do (BR05)",
    "KB07": "Nạp lô sau khi sao lưu → khôi phục đưa kho về mốc đã lưu (BR07)",
    "KB08": "Đổi hạng thành viên + giao dịch kỳ sau → báo cáo kỳ cũ không đổi (BR08)",
}


# ---------------------------------------------------------------- connections
@st.cache_resource
def ch():
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", "8123")),
        username=os.environ.get("CLICKHOUSE_USER", "bank"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "bank"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "false").lower() == "true")


def pg():
    conn = psycopg2.connect(host=os.environ.get("POSTGRES_HOST", "postgres"),
                            dbname=os.environ.get("POSTGRES_DB", "bankops"),
                            user=os.environ.get("POSTGRES_USER", "bank"),
                            password=os.environ.get("POSTGRES_PASSWORD", "bank"))
    conn.autocommit = True
    return conn


@st.cache_data(ttl=60, show_spinner=False)
def q_ch(sql, **params):
    """ClickHouse -> DataFrame of Python objects (dates up to 2299 do not fit pandas timestamps).
    Cached 60 s: Streamlit re-runs the whole script (every tab) on each click."""
    r = ch().query(sql, parameters=params or None, settings={"max_threads": 2})
    return pd.DataFrame(r.result_rows, columns=r.column_names)


def q_pg(sql, params=None):
    with pg() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        return pd.DataFrame(cur.fetchall(), columns=[c.name for c in cur.description])


def runner(path, body=None):
    req = urllib.request.Request(f"{RUNNER}{path}", data=json.dumps(body).encode() if body else None,
                                 headers={"Content-Type": "application/json"},
                                 method="POST" if body is not None else "GET")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except urllib.error.HTTPError as e:
        return {"error": json.loads(e.read()).get("error", str(e))}
    except Exception as e:  # noqa: BLE001
        return {"error": str(e)}


def run_and_wait(action, arg=None, label=""):
    """Send a command to the pipeline and show its output while it runs."""
    res = runner("/jobs", {"action": action, "arg": arg})
    if "error" in res:
        st.error(f"Không chạy được: {res['error']}")
        return None
    box, out = st.status(f"{label or action} đang chạy…", expanded=True), st.empty()
    while True:
        job = runner(f"/jobs/{res['job_id']}")
        out.code(job.get("output") or "…", language=None)
        if job.get("status") != "running":
            break
        time.sleep(2)
    ok = job.get("status") == "succeeded"
    box.update(label=f"{label or action}: {'xong' if ok else 'thất bại'}", state="complete" if ok else "error")
    st.cache_data.clear()
    return job


def money(x):
    return f"{float(x or 0):,.0f}".replace(",", ".")


# No FINAL on the fact table: the pipeline never loads a transaction_id twice (check U3); FINAL
# on 1M rows costs memory the 2.5 GB ClickHouse container does not have to spare.
FACT = """(SELECT f.*, toDate(toString(f.date_key)) AS d, p.cash_flow_direction AS dir,
                  p.type_name, p.operation_name, p.purpose_name
           FROM dw.fact_transaction AS f
           JOIN (SELECT * FROM dw.dim_transaction_profile FINAL) AS p USING (transaction_profile_sk))"""


# ---------------------------------------------------------------- header and sidebar
st.title("🏦 BankDW — Dòng tiền và hoạt động tài khoản")
st.caption("Kimball trên ClickHouse · kho vận hành PostgreSQL · dữ liệu Berka (Séc, 1993–1998) + hạng thành viên mô phỏng")

with st.sidebar:
    st.subheader("Trạng thái hệ thống")
    try:
        n = q_ch("SELECT count() AS n FROM dw.fact_transaction")["n"][0]
        st.success(f"Data Warehouse · {n:,} giao dịch".replace(",", "."))
    except Exception as e:  # noqa: BLE001
        st.error(f"Data Warehouse: {e}")
    try:
        open_alerts = q_pg("SELECT count(*) AS n FROM ops.alert WHERE status <> 'RESOLVED'")["n"][0]
        st.success(f"Kho vận hành · {open_alerts:,} cảnh báo chưa xử lý".replace(",", "."))
    except Exception as e:  # noqa: BLE001
        st.error(f"Kho vận hành: {e}")
    health = runner("/health")
    if "error" in health:
        st.warning("Pipeline: không kết nối được")
    else:
        st.info("Pipeline: đang chạy một tác vụ" if health.get("busy") else "Pipeline: sẵn sàng")

overview, analysis, operations, chatbot = st.tabs(["📊 Tổng quan", "🔎 Phân tích", "⚙️ Vận hành", "💬 Chatbot"])

# ================================================================ TỔNG QUAN (U01, BR01)
with overview:
    st.subheader("BR01 · Tổng quan dòng tiền")
    st.caption("Người dùng: Quản lý kinh doanh · Tiền vào / ra = SUM(amount) theo chiều tiền của loại giao dịch; "
               "dòng tiền thuần = vào − ra; hạng = hạng của chủ tài khoản tại ngày giao dịch (SCD2)")
    c1, c2, c3 = st.columns([2, 1, 2])
    period = c1.date_input("Kỳ báo cáo", (dt.date(1998, 1, 1), dt.date(1998, 12, 31)),
                           min_value=dt.date(1993, 1, 1), max_value=dt.date(1999, 12, 31), key="br01_period")
    grain = c2.radio("Biểu đồ theo", ["Tháng", "Ngày"], horizontal=True, key="br01_grain")
    tiers = c3.multiselect("Hạng", TIERS, default=TIERS, key="br01_tiers")
    if isinstance(period, tuple) and len(period) == 2 and tiers:
        lo, hi = (int(d.strftime("%Y%m%d")) for d in period)
        base = f"""FROM {FACT} AS t
                   JOIN (SELECT customer_sk, membership_tier FROM dw.dim_customer FINAL) AS c USING (customer_sk)
                   WHERE t.date_key BETWEEN %(lo)s AND %(hi)s AND c.membership_tier IN %(tiers)s"""
        k = q_ch(f"SELECT count() AS n, sumIf(amount, dir='IN') AS m_in, sumIf(amount, dir='OUT') AS m_out {base}",
                 lo=lo, hi=hi, tiers=tuple(tiers)).iloc[0]
        a, b, c, d = st.columns(4)
        a.metric("Tiền vào (CZK)", money(k.m_in))
        b.metric("Tiền ra (CZK)", money(k.m_out))
        c.metric("Dòng tiền thuần (CZK)", money(float(k.m_in or 0) - float(k.m_out or 0)))
        d.metric("Số giao dịch", money(k.n))
        bucket = "toStartOfMonth(t.d)" if grain == "Tháng" else "t.d"
        ts = q_ch(f"""SELECT {bucket} AS ky, sumIf(amount, dir='IN') AS tien_vao, sumIf(amount, dir='OUT') AS tien_ra,
                             count() AS so_giao_dich {base} GROUP BY ky ORDER BY ky""",
                  lo=lo, hi=hi, tiers=tuple(tiers))
        if len(ts):
            ts["ky"] = pd.to_datetime(ts["ky"])
            long = ts.melt("ky", ["tien_vao", "tien_ra"], "chieu", "so_tien")
            long["so_tien"] = long["so_tien"].astype(float)
            long["chieu"] = long["chieu"].map({"tien_vao": "Tiền vào", "tien_ra": "Tiền ra"})
            st.altair_chart(alt.Chart(long).mark_line(point=grain == "Tháng").encode(
                x=alt.X("ky:T", title=None), y=alt.Y("so_tien:Q", title="CZK"),
                color=alt.Color("chieu:N", title=None, scale=alt.Scale(domain=["Tiền vào", "Tiền ra"],
                                                                        range=["#2E7D32", "#C62828"])),
                tooltip=["ky:T", "chieu:N", alt.Tooltip("so_tien:Q", format=",.0f")]).properties(height=280),
                use_container_width=True)
            ts["so_giao_dich"] = ts["so_giao_dich"].astype(int)
            st.altair_chart(alt.Chart(ts).mark_bar(color="#5C6BC0").encode(
                x=alt.X("ky:T", title=None), y=alt.Y("so_giao_dich:Q", title="Số giao dịch"),
                tooltip=["ky:T", "so_giao_dich:Q"]).properties(height=160), use_container_width=True)
        by_tier = q_ch(f"""SELECT c.membership_tier AS hang, count() AS so_giao_dich,
                                  sumIf(amount, dir='IN') AS tien_vao, sumIf(amount, dir='OUT') AS tien_ra,
                                  tien_vao - tien_ra AS dong_tien_thuan
                           {base} GROUP BY hang""", lo=lo, hi=hi, tiers=tuple(tiers))
        if by_tier is not None and not by_tier.empty and "hang" in by_tier.columns:
            by_tier["hang"] = pd.Categorical(by_tier["hang"], TIERS, ordered=True)
            st.markdown("**Theo hạng thành viên**")
            st.dataframe(by_tier.sort_values("hang").style.format(
                {c: lambda v: money(v) for c in ["so_giao_dich", "tien_vao", "tien_ra", "dong_tien_thuan"]}),
                hide_index=True, use_container_width=True)
        else:
            st.info("Chưa có dữ liệu theo hạng thành viên trong kỳ báo cáo này.")

# ================================================================ PHÂN TÍCH (U02, BR02-BR04)
with analysis:
    sub_lookup, sub_alerts = st.tabs(["BR02 · Tra cứu tài khoản", "BR03 + BR04 · Cảnh báo"])

    with sub_lookup:
        st.caption("Người dùng: Chuyên viên phân tích · chọn mã tài khoản và khoảng thời gian")
        c1, c2 = st.columns([1, 2])
        acc = c1.number_input("Mã tài khoản (account_id)", min_value=1, value=11, step=1, key="br02_acc")
        rng = c2.date_input("Khoảng thời gian", (dt.date(1998, 7, 1), dt.date(1999, 12, 31)),
                            min_value=dt.date(1993, 1, 1), max_value=dt.date(1999, 12, 31), key="br02_rng")
        info = q_ch("""SELECT a.account_id, a.account_open_date, a.statement_frequency_name, d.district_name, d.region_name
                       FROM dw.dim_account AS a FINAL
                       JOIN (SELECT * FROM dw.dim_district FINAL) AS d ON d.district_sk = a.account_district_sk
                       WHERE a.account_id = %(a)s""", a=int(acc))
        if not len(info):
            st.warning("Không có tài khoản này trong kho.")
        elif isinstance(rng, tuple) and len(rng) == 2:
            i = info.iloc[0]
            st.markdown(f"**Tài khoản {i.account_id}** · mở ngày {i.account_open_date:%d/%m/%Y} · sao kê "
                        f"{i.statement_frequency_name.lower()} · chi nhánh {i.district_name} ({i.region_name})")
            lo, hi = (int(d.strftime("%Y%m%d")) for d in rng)
            tx = q_ch(f"""SELECT t.d AS ngay, t.transaction_id AS ma_giao_dich, t.dir AS chieu, t.type_name AS loai,
                                 t.operation_name AS phuong_thuc, t.purpose_name AS muc_dich, t.amount AS so_tien,
                                 t.balance_after AS so_du_sau, c.client_id AS chu_tai_khoan, c.membership_tier AS hang,
                                 t.load_batch_id AS lo_nap
                          FROM {FACT} AS t
                          JOIN (SELECT account_sk, account_id FROM dw.dim_account FINAL) AS a USING (account_sk)
                          JOIN (SELECT customer_sk, client_id, membership_tier FROM dw.dim_customer FINAL) AS c USING (customer_sk)
                          WHERE a.account_id = %(a)s AND t.date_key BETWEEN %(lo)s AND %(hi)s
                          ORDER BY t.date_key, t.transaction_id""", a=int(acc), lo=lo, hi=hi)
            m_in = tx.loc[tx.chieu == "IN", "so_tien"].astype(float).sum()
            m_out = tx.loc[tx.chieu == "OUT", "so_tien"].astype(float).sum()
            a, b, c, d = st.columns(4)
            a.metric("Tiền vào", money(m_in))
            b.metric("Tiền ra", money(m_out))
            c.metric("Thuần", money(m_in - m_out))
            d.metric("Số giao dịch", len(tx))
            st.dataframe(tx, hide_index=True, use_container_width=True, height=380,
                         column_config={"so_tien": st.column_config.NumberColumn(format="%.2f"),
                                        "so_du_sau": st.column_config.NumberColumn(format="%.2f")})

    with sub_alerts:
        st.caption("BR03: cảnh báo sinh sau mỗi lần nạp, kèm lý do và căn cứ · BR04: ghi nhận trạng thái và ghi chú")
        c1, c2, c3 = st.columns(3)
        f_status = c1.multiselect("Trạng thái", list(STATUS_VI), default=["NEW", "IN_REVIEW"],
                                  format_func=STATUS_VI.get, key="al_status")
        f_rule = c2.multiselect("Quy tắc", list(RULE_VI), default=list(RULE_VI), format_func=RULE_VI.get, key="al_rule")
        f_acc = c3.text_input("Lọc theo mã tài khoản", key="al_acc")
        if f_status and f_rule:
            sql = """SELECT alert_id, rule_code, account_id, period_start, status, reason, batch_id, detected_at
                     FROM ops.alert WHERE status = ANY(%s) AND rule_code = ANY(%s)"""
            params = [f_status, f_rule]
            if f_acc.strip().isdigit():
                sql += " AND account_id = %s"
                params.append(int(f_acc))
            alerts_df = q_pg(sql + " ORDER BY detected_at DESC, alert_id DESC LIMIT 500", params)
            st.markdown(f"**{len(alerts_df)} cảnh báo** (tối đa 500, mới nhất trước)")
            show = alerts_df.assign(status=alerts_df.status.map(STATUS_VI), rule_code=alerts_df.rule_code.map(RULE_VI))
            pick = st.dataframe(show, hide_index=True, use_container_width=True, height=260,
                                on_select="rerun", selection_mode="single-row", key="al_table")
            rows = pick.selection.rows if pick and pick.selection else []
            if rows:
                al = alerts_df.iloc[rows[0]]
                full = q_pg("SELECT * FROM ops.alert WHERE alert_id = %s", [int(al.alert_id)]).iloc[0]
                st.markdown(f"### Cảnh báo #{full.alert_id} · tài khoản {full.account_id} · {RULE_VI[full.rule_code]}")
                st.write(full.reason)
                c1, c2 = st.columns(2)
                c1.markdown("**Căn cứ**")
                c1.json(full.evidence)
                c2.markdown("**Giao dịch liên quan**")
                ids = list(full.transaction_ids)
                if ids:
                    c2.dataframe(q_ch(f"""SELECT t.d AS ngay, t.transaction_id, t.dir AS chieu, t.operation_name AS phuong_thuc,
                                                 t.amount AS so_tien, t.balance_after AS so_du_sau
                                          FROM {FACT} AS t WHERE t.transaction_id IN %(ids)s ORDER BY t.date_key""",
                                      ids=tuple(int(x) for x in ids)), hide_index=True, use_container_width=True)
                st.markdown("**BR04 · Xử lý cảnh báo**")
                with st.form(f"act_{full.alert_id}"):
                    c1, c2, c3 = st.columns([1, 2, 1])
                    to = c1.selectbox("Chuyển sang", list(STATUS_VI), index=list(STATUS_VI).index(full.status),
                                      format_func=STATUS_VI.get)
                    note = c2.text_input("Ghi chú kết quả")
                    who = c3.text_input("Người xử lý", value="analyst")
                    if st.form_submit_button("Ghi nhận"):
                        try:
                            q_pg("SELECT ops.set_alert_status(%s, %s, %s, %s) AS action_id",
                                 [int(full.alert_id), to, note or None, who or "analyst"])
                            st.success(f"Đã ghi nhận: {STATUS_VI[full.status]} → {STATUS_VI[to]}")
                        except Exception as e:  # noqa: BLE001
                            st.error(str(e).split("CONTEXT")[0])
                hist = q_pg("""SELECT acted_at, from_status, to_status, note, acted_by FROM ops.alert_action
                               WHERE alert_id = %s ORDER BY action_id""", [int(full.alert_id)])
                if len(hist):
                    hist["from_status"] = hist.from_status.map(STATUS_VI)
                    hist["to_status"] = hist.to_status.map(STATUS_VI)
                    st.dataframe(hist, hide_index=True, use_container_width=True)

# ================================================================ VẬN HÀNH (U03, BR05-BR08)
with operations:
    sub_runs, sub_dq, sub_kb, sub_backup, sub_hist = st.tabs(
        ["BR06 · Giám sát pipeline", "BR05 · Kiểm soát dữ liệu", "Chạy kịch bản (KB)",
         "BR07 · Sao lưu / khôi phục", "BR08 · Báo cáo lịch sử (SCD2)"])

    with sub_runs:
        runs = q_pg("""SELECT run_id, batch_id, run_kind, status, started_at, finished_at,
                              round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS giay,
                              rows_received, rows_loaded, rows_rejected, failed_step, error_message
                       FROM ops.pipeline_run ORDER BY run_id DESC LIMIT 200""")
        tot = q_pg("SELECT status, count(*) AS n FROM ops.pipeline_run GROUP BY status")
        st.write(" · ".join(f"**{r.status}**: {r.n}" for r in tot.itertuples()))
        pick = st.dataframe(runs, hide_index=True, use_container_width=True, height=300,
                            on_select="rerun", selection_mode="single-row", key="runs_table")
        rows = pick.selection.rows if pick and pick.selection else []
        if rows:
            r = runs.iloc[rows[0]]
            st.markdown(f"**Lần chạy #{r.run_id}** · lô `{r.batch_id}` · {r.run_kind} · **{r.status}**")
            if r.status == "FAILED":
                st.error(f"Bước thất bại: {r.failed_step}\n\n{r.error_message}")
            st.dataframe(q_pg("""SELECT step_order, step_name, status, started_at,
                                        round(extract(epoch FROM finished_at - started_at)::numeric, 2) AS giay,
                                        rows_in, rows_out, rows_rejected, error_message
                                 FROM ops.pipeline_step WHERE run_id = %s ORDER BY step_order""", [int(r.run_id)]),
                         hide_index=True, use_container_width=True)

    with sub_dq:
        rules = q_pg("SELECT rule_code, description FROM ops.dq_rule ORDER BY rule_code")
        summary = q_pg("""SELECT r.batch_id, r.run_id, x.rule_code, count(*) AS so_ban_ghi
                          FROM ops.rejected_record x JOIN ops.pipeline_run r USING (run_id)
                          GROUP BY 1, 2, 3 ORDER BY 2 DESC, 3""")
        c1, c2 = st.columns([1, 2])
        c1.markdown("**Quy tắc chất lượng**")
        c1.dataframe(rules, hide_index=True, use_container_width=True)
        c2.markdown("**Bản ghi bị tách theo lần chạy và quy tắc**")
        c2.dataframe(summary, hide_index=True, use_container_width=True)
        if len(summary):
            run_pick = st.selectbox("Xem bản ghi bị tách của lần chạy", sorted(summary.run_id.unique(), reverse=True))
            st.dataframe(q_pg("""SELECT rule_code, source_table, source_key, reason, record, rejected_at
                                 FROM ops.rejected_record WHERE run_id = %s ORDER BY rule_code, reject_id""",
                              [int(run_pick)]), hide_index=True, use_container_width=True)

    with sub_kb:
        st.caption("Luồng N01–N05: chọn và xem trước (chưa ghi Data Lake/kho) → chạy → đối chiếu mong đợi và thực tế → chạy lại")
        kb = st.selectbox("Kịch bản", list(KB_INFO), format_func=lambda k: f"{k} · {KB_INFO[k]}", key="kb_pick")
        pv = runner(f"/preview/{kb}")
        if "error" in pv:
            st.error(pv["error"])
        else:
            c1, c2 = st.columns(2)
            for name, f in pv["files"].items():
                c1.markdown(f"**{name}** · {f['rows']} dòng (10 dòng đầu)")
                c1.dataframe(pd.DataFrame(f["sample"], columns=f["header"]), hide_index=True, use_container_width=True)
            c2.markdown("**Kết quả mong đợi** (`expected.json`)")
            c2.json(pv.get("expected", {}), expanded=False)
            batch = pv.get("expected", {}).get("batch_id")
            before = q_pg("SELECT count(*) AS n FROM ops.pipeline_run WHERE batch_id = %s", [batch])["n"][0] if batch else 0
            st.write(f"Lô `{batch}` đã chạy **{before}** lần." + (" Chạy lần nữa = **chạy lại** (không nhân đôi dữ liệu)." if before else ""))
            if st.button(f"▶ Chạy {kb}", type="primary"):
                job = run_and_wait("scenario", kb, f"Kịch bản {kb}")
                if job and batch:
                    last = q_pg("""SELECT run_id, run_kind, status, rows_received, rows_loaded, rows_rejected
                                   FROM ops.pipeline_run WHERE batch_id = %s ORDER BY run_id DESC LIMIT 1""", [batch])
                    st.markdown("**Thực tế (lần chạy vừa xong)**")
                    st.dataframe(last, hide_index=True, use_container_width=True)
                    if kb == "KB03":
                        st.dataframe(q_pg("SELECT rule_code, account_id, period_start, status, reason FROM ops.alert "
                                          "WHERE batch_id = %s ORDER BY alert_id", [batch]), hide_index=True)
                    if kb == "KB05":
                        st.dataframe(q_pg("SELECT rule_code, count(*) AS n FROM ops.rejected_record WHERE run_id = %s "
                                          "GROUP BY 1 ORDER BY 1", [int(last.run_id[0])]), hide_index=True)

    with sub_backup:
        st.caption("Sao lưu cấu trúc và dữ liệu của 6 bảng DW (kể cả lịch sử SCD) vào vùng lưu riêng; chỉ chạy khi pipeline không ghi")
        c1, c2 = st.columns(2)
        with c1:
            name = st.text_input("Tên bản sao lưu (để trống = theo thời gian)", key="bk_name")
            if st.button("💾 Sao lưu"):
                run_and_wait("backup", name.strip() or None, "Sao lưu")
        cat = q_pg("""SELECT backup_id, status, created_at, (row_counts->>'fact_transaction')::bigint AS so_giao_dich,
                             (totals->'scd_versions'->>'total')::int AS phien_ban_scd
                      FROM ops.backup_catalog ORDER BY created_at DESC""")
        with c2:
            avail = cat.loc[cat.status == "AVAILABLE", "backup_id"].tolist()
            target = st.selectbox("Khôi phục về bản", avail, key="bk_target") if avail else None
            if target and st.button("⏪ Khôi phục", type="primary"):
                run_and_wait("restore", target, f"Khôi phục {target}")
        st.markdown("**Danh mục bản sao lưu**")
        st.dataframe(cat, hide_index=True, use_container_width=True)
        ops_df = q_pg("""SELECT op_id, backup_id, op_type, status, started_at,
                                round(extract(epoch FROM finished_at - started_at)::numeric, 1) AS giay,
                                result->'matches_backup' AS khop_ban_sao_luu, error_message
                         FROM ops.backup_operation ORDER BY op_id DESC""")
        st.markdown("**Lịch sử thao tác** (khôi phục: so số dòng, tiền vào/ra, báo cáo theo hạng, phiên bản SCD với bản sao lưu)")
        st.dataframe(ops_df, hide_index=True, use_container_width=True)

    with sub_hist:
        st.caption("Bảng hạng mô phỏng lưu hạng hiện tại + ngày hiệu lực; ETL giữ lịch sử bằng SCD Type 2. "
                   "Giao dịch kỳ cũ giữ hạng cũ, giao dịch từ ngày hiệu lực dùng hạng mới.")
        c1, c2 = st.columns(2)
        old = c1.date_input("Kỳ cũ", (dt.date(1993, 1, 1), dt.date(1998, 12, 31)), key="h_old")
        new = c2.date_input("Kỳ mới", (dt.date(1999, 1, 1), dt.date(1999, 1, 31)), key="h_new")

        def tier_report(rng):
            lo, hi = (int(d.strftime("%Y%m%d")) for d in rng)
            r = q_ch(f"""SELECT c.membership_tier AS hang, uniqExact(c.client_id) AS so_khach, count() AS so_giao_dich,
                                sumIf(amount, dir='IN') AS tien_vao, sumIf(amount, dir='OUT') AS tien_ra
                         FROM {FACT} AS t
                         JOIN (SELECT customer_sk, client_id, membership_tier FROM dw.dim_customer FINAL) AS c USING (customer_sk)
                         WHERE t.date_key BETWEEN %(lo)s AND %(hi)s GROUP BY hang""", lo=lo, hi=hi)
            cols = ["hang", "so_khach", "so_giao_dich", "tien_vao", "tien_ra"]
            if r is None or r.empty or "hang" not in r.columns:
                return pd.DataFrame(columns=cols)
            r["hang"] = pd.Categorical(r["hang"], TIERS, ordered=True)
            return r.sort_values("hang")

        if isinstance(old, tuple) and len(old) == 2:
            df_old = tier_report(old)
            if df_old.empty:
                c1.info("Không có dữ liệu giao dịch trong kỳ cũ.")
            else:
                c1.dataframe(df_old, hide_index=True, use_container_width=True)

        if isinstance(new, tuple) and len(new) == 2:
            df_new = tier_report(new)
            if df_new.empty:
                c2.info("Chưa có dữ liệu kỳ mới (chỉ xuất hiện sau khi chạy kịch bản KB08).")
            else:
                c2.dataframe(df_new, hide_index=True, use_container_width=True)
        scd = q_ch("""SELECT count() AS phien_ban, countIf(is_current = 1) AS hien_tai, countIf(is_current = 0) AS da_dong,
                             uniqExact(client_id) AS khach FROM dw.dim_customer FINAL""").iloc[0]
        st.write(f"`dim_customer`: **{scd.phien_ban:,}** phiên bản · {scd.hien_tai:,} hiện tại · {scd.da_dong:,} đã đóng · "
                 f"{scd.khach:,} khách".replace(",", "."))
        demo = st.number_input("Khách hàng demo (client_id) — xem các phiên bản hạng và giao dịch theo phiên bản",
                               min_value=1, value=24, step=1, key="h_client")
        vers = q_ch("""SELECT customer_sk, membership_tier AS hang, valid_from, valid_to, is_current, updated_at
                       FROM dw.dim_customer FINAL WHERE client_id = %(c)s ORDER BY valid_from""", c=int(demo))
        vers["valid_to"] = vers["valid_to"].map(lambda d: "hiện tại" if d.year == 2299 else d)
        st.dataframe(vers, hide_index=True, use_container_width=True)
        if len(vers):
            st.dataframe(q_ch(f"""SELECT c.membership_tier AS hang, t.customer_sk, min(t.d) AS tu_ngay, max(t.d) AS den_ngay,
                                         count() AS so_giao_dich, sumIf(amount, dir='IN') AS tien_vao,
                                         sumIf(amount, dir='OUT') AS tien_ra
                                  FROM {FACT} AS t
                                  JOIN (SELECT customer_sk, client_id, membership_tier FROM dw.dim_customer FINAL) AS c USING (customer_sk)
                                  WHERE c.client_id = %(c)s GROUP BY hang, t.customer_sk ORDER BY tu_ngay""", c=int(demo)),
                         hide_index=True, use_container_width=True)

with chatbot:
    st.subheader("💬 Trợ lý nghiệp vụ & kho dữ liệu BankDW")

    from rag_service import query_rag

    # Khởi tạo phiên trò chuyện trong session state
    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = [
            {"role": "assistant", "content": "Xin chào! Bạn có thể hỏi tôi về số liệu thống kê liên quan đến giao dịch, biến động số dư, phân hạng khách hàng... từ năm 1993 đến 1998"}
        ]

    # Hiển thị lịch sử tin nhắn
    for message in st.session_state.chat_messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])

    # Xử lý khi người dùng nhập câu hỏi
    if user_query := st.chat_input("Nhập câu hỏi..."):
        st.session_state.chat_messages.append({"role": "user", "content": user_query})
        with st.chat_message("user"):
            st.markdown(user_query)

        with st.chat_message("assistant"):
            with st.spinner("Đang tra cứu..."):
                reply = query_rag(user_query)
                st.markdown(reply)
        st.session_state.chat_messages.append({"role": "assistant", "content": reply})