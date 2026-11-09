import os
import re
import time
import clickhouse_connect
from google import genai

MODEL_NAME = os.environ.get("GEMINI_MODEL", "gemini-3.8-flash")

DW_SCHEMA_PROMPT = """
Dưới đây là cấu trúc Data Warehouse ClickHouse (database `dw`):
1. Bảng dw.fact_transaction (1.056.320 dòng, từ năm 1993 đến 1998):
   - transaction_id (UInt64)
   - date_key (UInt32, định dạng YYYYMMDD, ví dụ 19950615)
   - account_sk (UInt64)
   - customer_sk (UInt64)
   - transaction_profile_sk (UInt32)
   - amount (Decimal(18, 2))
   - balance_after (Decimal(18, 2))

2. Bảng dw.dim_customer (SCD Type 2):
   - customer_sk (UInt64)
   - client_id (UInt32)
   - membership_tier (String: 'Standard', 'Silver', 'Gold', 'Platinum')
   - valid_from (Date), valid_to (Date)
   - is_current (UInt8, 1 là bản ghi hiện hành, 0 là lịch sử)

3. Bảng dw.dim_account:
   - account_sk (UInt64)
   - account_id (UInt32)
   - district_id (UInt16)

4. Bảng dw.dim_transaction_profile:
   - transaction_profile_sk (UInt32)
   - type_code (String: 'CR' là tiền vào/Credit, 'DB' là tiền ra/Debit)
   - operation_code (String: 'CASH_WITHDRAWAL', 'CASH_DEPOSIT', 'COLLECTION_OTHER_BANK', 'REMITTANCE_OTHER_BANK', 'WITHDRAWAL')
   - purpose_code (String)
"""


def get_clickhouse_client():
    """Tạo kết nối tới ClickHouse Data Warehouse có cấu hình Timeout tránh kẹt Socket."""
    return clickhouse_connect.get_client(
        host=os.environ.get("CLICKHOUSE_HOST", "clickhouse"),
        port=int(os.environ.get("CLICKHOUSE_PORT", 8123)),
        username=os.environ.get("CLICKHOUSE_USER", "bank"),
        password=os.environ.get("CLICKHOUSE_PASSWORD", "bank"),
        database="dw",
        connect_timeout=10,
        send_receive_timeout=15,
    )


def execute_sql(sql_query: str) -> str:
    """Chạy câu truy vấn SELECT trên ClickHouse và trả về kết quả."""
    clean_sql = sql_query.strip().rstrip(";")
    if not clean_sql.upper().startswith("SELECT"):
        return "Lỗi: Chỉ cho phép thực hiện các truy vấn đọc dữ liệu (SELECT)."

    try:
        ch_client = get_clickhouse_client()
        df = ch_client.query_df(clean_sql)
        if df.empty:
            return "Không tìm thấy bản ghi nào thỏa mãn điều kiện."
        return df.head(30).to_string(index=False)
    except Exception as e:
        return f"Lỗi thực thi SQL: {str(e)}"


def query_rag(user_question: str) -> str:
    api_key = os.environ.get("GEMINI_API_KEY") or os.environ.get("GENAI_API_KEY")
    if not api_key:
        return "Chưa cấu hình biến môi trường `GEMINI_API_KEY` hoặc `GENAI_API_KEY`."

    ai_client = genai.Client(api_key=api_key)

    prompt_stage1 = f"""Bạn là trợ lý phân tích dữ liệu kho ngân hàng BankDW.

{DW_SCHEMA_PROMPT}

CÂU HỎI CỦA NGƯỜI DÙNG:
{user_question}

HƯỚNG DẪN TRẢ LỜI:
Hãy viết DUY NHẤT một câu lệnh ClickHouse SQL hợp lệ (bắt đầu bằng SELECT) để lấy số liệu trả lời cho câu hỏi trên.
Đặt câu truy vấn trong khối:
```sql
SELECT ...
"""

    res1 = None
    for attempt in range(2):
        try:
            res1 = ai_client.models.generate_content(
                model=MODEL_NAME,
                contents=prompt_stage1,
                config={"temperature": 0.0}
            )
            break
        except Exception as e:
            if attempt == 0 and ("503" in str(e) or "429" in str(e)):
                time.sleep(2)
                continue
            return f"Máy chủ AI đang bận ({str(e)}). Vui lòng thử lại sau vài giây."

    if not res1 or not res1.text:
        return "Mô hình AI không trả về nội dung. Vui lòng đặt lại câu hỏi."

    res_text = res1.text.strip()
    sql_match = re.search(r"```sql\s*(.*?)\s*```", res_text, re.DOTALL)
    if not sql_match:
        return res_text

    sql_query = sql_match.group(1).strip()
    query_result = execute_sql(sql_query)

    if query_result.startswith("Lỗi"):
        return f"{query_result}\n\n*Câu truy vấn:* `{sql_query}`"

    prompt_stage2 = f"""Người dùng đã hỏi: "{user_question}"

Kết quả trả về từ database ClickHouse:
{query_result}

Hãy diễn giải kết quả số liệu trên thành câu trả lời hoàn chỉnh, súc tích bằng tiếng Việt cho người dùng. 
"""

    try:
        res2 = ai_client.models.generate_content(model="gemini-3.8-flash", contents=prompt_stage2, config={"temperature": 0.0})
        return res2.text
    except Exception:
        return f"Kết quả từ hệ thống:\n{query_result}"