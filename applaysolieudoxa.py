import streamlit as st
import pandas as pd
import io
import os

# ================= 1. CẤU HÌNH =================
st.set_page_config(page_title="Tool SFW V77 - Strict TCC Counting", layout="wide")

# ================= 2. HÀM HỖ TRỢ =================
def safe_str(val):
    if pd.isna(val) or val is None: return ""
    s = str(val).strip().upper()
    if s.endswith(".0"): s = s[:-2]
    return s

def clean_station_code(val):
    s = safe_str(val)
    if s.startswith("PB"): s = s[2:]
    s = s.lstrip('0')
    return s

def get_left_13(val):
    return safe_str(val)[:13]

def find_header_row_and_read(file_obj, keywords):
    if file_obj is None: return None, 0
    try:
        file_obj.seek(0)
        is_csv = file_obj.name.lower().endswith('.csv')
        is_xls = file_obj.name.lower().endswith('.xls')
        try:
            if is_csv: df_preview = pd.read_csv(file_obj, header=None, nrows=15, dtype=str)
            elif is_xls: df_preview = pd.read_excel(file_obj, header=None, nrows=15, dtype=str, engine='xlrd')
            else: df_preview = pd.read_excel(file_obj, header=None, nrows=15, dtype=str)
        except: return None, 0
        
        header_row_idx = 0
        found = False
        for i, row in df_preview.iterrows():
            row_text = " ".join([str(x).upper() for x in row.values])
            for kw in keywords:
                if kw.upper() in row_text:
                    header_row_idx = i
                    found = True; break
            if found: break
        
        file_obj.seek(0)
        if is_csv: df = pd.read_csv(file_obj, header=header_row_idx, dtype=str)
        elif is_xls: df = pd.read_excel(file_obj, header=header_row_idx, dtype=str, engine='xlrd')
        else: df = pd.read_excel(file_obj, header=header_row_idx, dtype=str)
        df.columns = [str(c).strip().upper() for c in df.columns]
        return df, header_row_idx
    except: return None, 0

def find_col(df, keywords):
    if df is None: return None
    for kw in keywords:
        for col in df.columns:
            if kw in col: return col
    return None

def load_data_full_dict(file_list):
    result_dict = {}
    if not file_list: return result_dict
    for f in file_list:
        df_tmp, _ = find_header_row_and_read(f, ["MADIEMDO", "MÃ ĐIỂM ĐO", "Mã điểm đo"])
        if df_tmp is not None:
            c_ma = find_col(df_tmp, ["MADIEMDO", "MÃ ĐIỂM ĐO", "Mã điểm đo"])
            c_tt = find_col(df_tmp, ["TRANGTHAI", "TRẠNG THÁI", "Trạng thái"])
            if c_ma and c_tt:
                for index, row in df_tmp.iterrows():
                    code = get_left_13(row[c_ma])
                    # Fix: Bỏ qua các dòng không có mã điểm đo
                    if code != "":
                        status = str(row[c_tt]).strip()
                        result_dict[code] = status
    return result_dict

# ================= 3. LOGIC TỔNG HỢP V77 =================
def create_summaries(df_tcd, df_tcc, ma_dvi_filter):
    if ma_dvi_filter:
        tcd_calc = df_tcd[df_tcd['MA_DVIQLY'].astype(str).str.startswith(ma_dvi_filter)].copy()
        tcc_calc = df_tcc[df_tcc['MA_DVIQLY'].astype(str).str.startswith(ma_dvi_filter)].copy()
    else:
        tcd_calc = df_tcd.copy()
        tcc_calc = df_tcc.copy()
    
    # === 1. TỔNG HỢP TCD ===
    tcd_calc['Flag_Modem_Data'] = tcd_calc['STT_MODEM'].apply(lambda x: 1 if "CÓ DỮ LIỆU" in safe_str(x) else 0)
    tcd_calc['Flag_DCU'] = tcd_calc['DCU'].apply(lambda x: 1 if x == 'DCU' else 0)
    tcd_calc['Flag_MD'] = tcd_calc['MD'].apply(lambda x: 1 if x == 'MD' else 0)

    summary_tcd = tcd_calc.groupby('MA_DVIQLY').agg(
        Tong_TCD_CMIS=('MA_SO', 'count'),
        Co_MD=('Flag_MD', 'sum'), 
        Co_DCU=('Flag_DCU', 'sum'),
        Modem_Co_Du_Lieu=('Flag_Modem_Data', 'sum')
    ).fillna(0).astype(int)

    summary_tcd['Chua_Co_Do_Xa'] = summary_tcd['Tong_TCD_CMIS'] - (summary_tcd['Co_DCU'] + summary_tcd['Modem_Co_Du_Lieu'])
    summary_tcd['Chua_Co_Do_Xa'] = summary_tcd['Chua_Co_Do_Xa'].clip(lower=0)
    
    summary_tcd.columns = ['Tổng số TCD', 'Có MD', 'Có DCU', 'Modem có dữ liệu', 'Chưa có đo xa']
    total_tcd = summary_tcd.sum(numeric_only=True)
    total_tcd.name = 'TỔNG CỘNG'
    summary_tcd = pd.concat([summary_tcd, total_tcd.to_frame().T])
    summary_tcd = summary_tcd.reset_index().rename(columns={'index': 'Đơn Vị'})

    # === 2. TỔNG HỢP TCC ===
    tcc_calc = tcc_calc[tcc_calc['LOAI_TRAM'] == 'CC'].copy()
    
    tcc_calc['Count_Is_CTT'] = tcc_calc['CTT'].apply(lambda x: 1 if x == 'CTT' else 0)
    tcc_calc['Count_CTT_Has_Data'] = tcc_calc['STT_CTT'].apply(lambda x: 1 if "CÓ DỮ LIỆU" in safe_str(x) else 0)
    tcc_calc['Count_MD_Has_Data'] = tcc_calc['STT_MODEM'].apply(lambda x: 1 if "CÓ DỮ LIỆU" in safe_str(x) else 0)

    def is_really_modem_offline(row):
        is_ctt = (row['CTT'] == 'CTT')
        has_data = ("CÓ DỮ LIỆU" in safe_str(row['STT_MODEM']))
        has_md = (row['MD'] == 'MD') 
        
        if not is_ctt and not has_data and has_md:
            return 1
        return 0

    tcc_calc['Count_MD_Offline'] = tcc_calc.apply(is_really_modem_offline, axis=1)

    summary_tcc = tcc_calc.groupby('MA_DVIQLY').agg(
        Tong_Tram_CC=('MA_SO', 'count'),            
        CTT_Co_Do_Xa=('Count_Is_CTT', 'sum'),        
        MD_Co_Du_Lieu=('Count_MD_Has_Data', 'sum'),  
        CTT_Co_Status=('Count_CTT_Has_Data', 'sum'),
        Modem_Offline=('Count_MD_Offline', 'sum')
    ).fillna(0).astype(int)
    
    summary_tcc['CTT_Chua_Do_Xa'] = summary_tcc['CTT_Co_Do_Xa'] - summary_tcc['MD_Co_Du_Lieu']
    summary_tcc['CTT_Chua_Do_Xa'] = summary_tcc['CTT_Chua_Do_Xa'].clip(lower=0)
    
    summary_tcc['Numerator'] = summary_tcc['CTT_Co_Status'] + summary_tcc['MD_Co_Du_Lieu']
    summary_tcc['Denominator'] = summary_tcc['CTT_Co_Do_Xa'] + summary_tcc['MD_Co_Du_Lieu'] + summary_tcc['Modem_Offline']
    
    summary_tcc['Ty_Le'] = summary_tcc.apply(
        lambda x: (x['Numerator'] / x['Denominator'] * 100) if x['Denominator'] > 0 else 0, 
        axis=1
    )
    summary_tcc = summary_tcc.drop(columns=['Numerator', 'Denominator'])
    
    summary_tcc.columns = [
        'Tổng số trạm CC', 'Tổng CTT', 'MD có dữ liệu', 'CTT có trạng thái DL',
        'Modem Offline', 'CTT chưa đo xa', 'Tỷ lệ thu thập (%)'
    ]
    
    total_tcc = summary_tcc.sum(numeric_only=True)
    total_tcc.name = 'TỔNG CỘNG'
    
    num_total = total_tcc['CTT có trạng thái DL'] + total_tcc['MD có dữ liệu']
    den_total = total_tcc['Tổng CTT'] + total_tcc['MD có dữ liệu'] + total_tcc['Modem Offline']
    total_tcc['Tỷ lệ thu thập (%)'] = (num_total / den_total * 100) if den_total > 0 else 0
    
    summary_tcc = pd.concat([summary_tcc, total_tcc.to_frame().T])
    summary_tcc = summary_tcc.reset_index().rename(columns={'index': 'Đơn Vị'})

    return summary_tcd, summary_tcc

# ================= 4. XUẤT EXCEL =================
def to_excel_4_sheets(df_tcd, df_tcc, sum_tcd, sum_tcc):
    output = io.BytesIO()
    rename_dict = {
        'MA_SO': 'MÃ KH/ĐĐ', 
        'MA_CLOAI': 'MÃ CHỦNG LOẠI', 
        'STT_MODEM': 'TRẠNG THÁI MODEM', 
        'STT_CTT': 'GHI CHÚ DỮ LIỆU CTT', 
        'CTT': 'DANH SÁCH CTT',
        'MD': 'CÓ MD', 
        'DCU': 'CÓ DCU',
        'IMEI_MD': 'IMEI (MODEM)',
        'METHOD_CTT': 'PHƯƠNG THỨC CTT',
        'SERIAL_SIM': 'SERIAL SIM',
        'SDT_SIM': 'SĐT SIM'
    }

    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        workbook = writer.book
        fmt_red = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
        fmt_yellow = workbook.add_format({'bg_color': '#FFEB9C', 'font_color': '#9C6500'})
        fmt_green = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
        fmt_blue = workbook.add_format({'bg_color': '#BDD7EE', 'font_color': '#000000'})
        fmt_purple = workbook.add_format({'bg_color': '#E4C7FA', 'font_color': '#333333'})
        fmt_grey = workbook.add_format({'bg_color': '#D9D9D9', 'font_color': '#595959', 'italic': True})
        
        fmt_header = workbook.add_format({'bold': True, 'border': 1, 'bg_color': '#D3D3D3', 'align': 'center', 'valign': 'vcenter'})
        fmt_sum_header_tcd = workbook.add_format({'bold': True, 'border': 1, 'bg_color': '#4472C4', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter'})
        fmt_sum_header_tcc = workbook.add_format({'bold': True, 'border': 1, 'bg_color': '#70AD47', 'font_color': 'white', 'align': 'center', 'valign': 'vcenter'})
        fmt_percent = workbook.add_format({'num_format': '0.00'})

        def write_detail(df_in, name, drop_cols=None):
            if df_in is None: return
            df_temp = df_in.copy()
            if drop_cols: df_temp = df_temp.drop(columns=drop_cols, errors='ignore')
            
            cols = list(df_temp.columns)
            if 'NHAN_XET' in cols:
                cols.insert(1, cols.pop(cols.index('NHAN_XET')))
            df_temp = df_temp[cols]

            df_display = df_temp.rename(columns=rename_dict)
            df_display.to_excel(writer, index=False, sheet_name=name)
            ws = writer.sheets[name]
            for i, c in enumerate(df_display.columns):
                ws.write(0, i, c, fmt_header)
                ws.set_column(i, i, 22)
            try:
                col_idx = df_display.columns.get_loc("NHAN_XET")
                L = chr(65 + col_idx)
                R = f"{L}2:{L}{len(df_display)+1}"
                ws.conditional_format(R, {'type': 'text', 'criteria': 'containing', 'value': 'Chưa khai báo', 'format': fmt_red})
                ws.conditional_format(R, {'type': 'text', 'criteria': 'containing', 'value': 'Modem Offline', 'format': fmt_yellow})
                ws.conditional_format(R, {'type': 'text', 'criteria': 'containing', 'value': 'có dữ liệu', 'format': fmt_green})
                ws.conditional_format(R, {'type': 'text', 'criteria': 'containing', 'value': 'CTT chưa thu thập', 'format': fmt_blue})
                ws.conditional_format(R, {'type': 'text', 'criteria': 'containing', 'value': 'Lỗi CTT', 'format': fmt_purple})
                
                ws.conditional_format(R, {'type': 'text', 'criteria': 'containing', 'value': 'Thu hồi Modem', 'format': fmt_grey})
                
                if "TRẠNG THÁI MODEM" in df_display.columns:
                    col_idx_md = df_display.columns.get_loc("TRẠNG THÁI MODEM")
                    L_md = chr(65 + col_idx_md)
                    R_md = f"{L_md}2:{L_md}{len(df_display)+1}"
                    ws.conditional_format(R_md, {'type': 'text', 'criteria': 'containing', 'value': 'Có dữ liệu', 'format': fmt_green})
                
                if "GHI CHÚ DỮ LIỆU CTT" in df_display.columns:
                    col_idx_ctt = df_display.columns.get_loc("GHI CHÚ DỮ LIỆU CTT")
                    L_ctt = chr(65 + col_idx_ctt)
                    R_ctt = f"{L_ctt}2:{L_ctt}{len(df_display)+1}"
                    ws.conditional_format(R_ctt, {'type': 'text', 'criteria': 'containing', 'value': 'Có dữ liệu', 'format': fmt_green})
            except: pass

        write_detail(df_tcd, 'ChuyenDung', drop_cols=['STT_CTT'])
        write_detail(df_tcc, 'NoiBo')

        if sum_tcd is not None:
            sum_tcd.to_excel(writer, index=False, sheet_name='TongHop_TCD')
            ws = writer.sheets['TongHop_TCD']
            for i, c in enumerate(sum_tcd.columns):
                ws.write(0, i, c, fmt_sum_header_tcd)
                ws.set_column(i, i, 18)

        if sum_tcc is not None:
            sum_tcc.to_excel(writer, index=False, sheet_name='TongHop_TCC')
            ws = writer.sheets['TongHop_TCC']
            for i, c in enumerate(sum_tcc.columns):
                ws.write(0, i, c, fmt_sum_header_tcc)
                ws.set_column(i, i, 20)
            last_col_idx = len(sum_tcc.columns) - 1
            ws.set_column(last_col_idx, last_col_idx, 15, fmt_percent)

    return output.getvalue()

# ================= 5. GIAO DIỆN CHÍNH =================
st.title("⚡ Tool SFW V77 (Strict Modem Offline Count)")

st.markdown("### ⚙️ Cấu hình bộ lọc")
ma_dvi_filter = st.text_input("🔍 Nhập Mã Đơn Vị cần lọc (VD: PB06, PB0606... Để trống để tổng hợp tất cả):", value="PB06").strip().upper()
st.markdown("---")

c1, c2 = st.columns([1, 1.2])
with c1:
    st.header("1. File Input")
    f_tcd = st.file_uploader("📂 File TCD", type=['xlsx','csv'])
    f_tcc = st.file_uploader("📂 File TCC", type=['xlsx','csv'])
with c2:
    st.header("2. Dữ Liệu & Hệ Thống")
    f_md = st.file_uploader("1. Modem All", type=['xlsx','csv'])
    
    st.markdown("---")
    f_data_modem = st.file_uploader("2a. Dữ Liệu MODEM", type=['xlsx','csv', 'xls'], accept_multiple_files=True)
    f_data_ctt = st.file_uploader("2b. Dữ Liệu CTT", type=['xlsx','csv', 'xls'], accept_multiple_files=True)
    st.markdown("---")
    
    f_dc = st.file_uploader("3. DCU All", type=['xlsx','csv'])
    f_ct = st.file_uploader("4. CTT All", type=['xlsx','csv'])

if st.button("🚀 XỬ LÝ NGAY", type="primary"):
    if not (f_md and f_data_modem and f_data_ctt and f_dc and f_ct and f_tcd and f_tcc):
        st.error("Thiếu file!"); st.stop()

    try:
        dict_modem_full = load_data_full_dict(f_data_modem) 
        dict_ctt_full = load_data_full_dict(f_data_ctt)
        st.success(f"✅ Đã tải dữ liệu.")

        # --- XỬ LÝ FILE 1: MODEM ALL ---
        df_md, _ = find_header_row_and_read(f_md, ["MADIEMDO"])
        c_md = find_col(df_md, ["MADIEMDO"])
        c_imei = find_col(df_md, ["IMEI"])
        c_serialid = find_col(df_md, ["SERIALID"])
        c_metertype = find_col(df_md, ["METERTYPE"])
        c_sim_md = find_col(df_md, ["SERIALSIM", "SERIAL_SIM"])
        
        dict_md_info = {}
        if c_md:
            for _, row in df_md.iterrows():
                md_code = get_left_13(row[c_md])
                # Fix: Chỉ thêm vào dict nếu md_code có giá trị
                if md_code != "":
                    dict_md_info[md_code] = {
                        'imei': safe_str(row[c_imei]) if c_imei else "",
                        'serial': safe_str(row[c_serialid]) if c_serialid else "",
                        'metertype': safe_str(row[c_metertype]) if c_metertype else "",
                        'sim': safe_str(row[c_sim_md]) if c_sim_md else ""
                    }
        s_md = set(dict_md_info.keys())

        # --- XỬ LÝ FILE 3: DCU ALL ---
        df_dc, _ = find_header_row_and_read(f_dc, ["MATRAM"])
        c_dc = find_col(df_dc, ["MATRAM"])
        c_sdt_dc = find_col(df_dc, ["SDT_SIM", "SDTSIM", "SĐT"])
        
        dict_dc_sdt = {}
        if c_dc:
            for _, row in df_dc.iterrows():
                dc_code = clean_station_code(row[c_dc])
                # Fix: Chỉ xử lý nếu mã trạm khác rỗng
                if dc_code != "":
                    val_sdt = safe_str(row[c_sdt_dc]) if c_sdt_dc else ""
                    dict_dc_sdt[dc_code] = val_sdt
        s_dc = set(dict_dc_sdt.keys())

        # --- XỬ LÝ FILE 4: CTT ALL ---
        df_ct, _ = find_header_row_and_read(f_ct, ["MADIEMDO", "MATRAM", "TENTRAM"])
        c1_ct = find_col(df_ct, ["MADIEMDO"])
        c2_ct = find_col(df_ct, ["TENTRAM", "MATRAM"])
        c_method = find_col(df_ct, ["METHOD", "PHƯƠNG THỨC", "PHUONG THUC"])
        
        dict_ct_method = {}
        if c1_ct and c_method:
            for _, row in df_ct.iterrows():
                md_code = get_left_13(row[c1_ct])
                # Fix: Tránh rỗng
                if md_code != "":
                    dict_ct_method[md_code] = safe_str(row[c_method])
                
        # Fix: Trích xuất set CTT loại bỏ chuỗi rỗng
        s_ct1 = set(x for x in df_ct[c1_ct].apply(get_left_13) if x != "") if c1_ct else set()
        s_ct2 = set(x for x in df_ct[c2_ct].apply(clean_station_code) if x != "") if c2_ct else set()

        def process(f, type_):
            df, _ = find_header_row_and_read(f, ["MA_KHANG", "MA_DDO", "MA_KHACH_HANG"])
            if df is None: return None
            
            c_dvi = find_col(df, ["MA_DVIQLY"])
            c_kh = find_col(df, ["MA_KHANG", "MA_DDO"])
            c_tram = find_col(df, ["MA_TRAM"])
            c_ten = find_col(df, ["TEN_KHANG", "TEN_DDO"])
            c_loai = find_col(df, ["LOAI_TRAM"])
            c_cloai = find_col(df, ["MA_CLOAI", "CHUNG_LOAI"])
            
            if not c_kh: return None
            
            out = pd.DataFrame()
            out['MA_DVIQLY'] = df[c_dvi].apply(safe_str) if c_dvi else "UNKNOWN"
            out['MA_SO'] = df[c_kh].apply(safe_str)
            out['MA_CLOAI'] = df[c_cloai].apply(safe_str) if c_cloai else ""
            out['MA_TRAM'] = df[c_tram].apply(safe_str) if c_tram else ""
            out['TEN'] = df[c_ten].apply(safe_str) if c_ten else ""
            out['LOAI_TRAM'] = df[c_loai].apply(safe_str) if c_loai else type_
            
            k13 = out['MA_SO'].apply(get_left_13)
            ktram = out['MA_TRAM'].apply(clean_station_code)
            
            out['IMEI_MD'] = k13.apply(lambda x: dict_md_info.get(x, {}).get('imei', '') if x != "" else "")
            out['METHOD_CTT'] = k13.apply(lambda x: dict_ct_method.get(x, "") if x != "" else "")
            out['SERIAL_SIM'] = k13.apply(lambda x: dict_md_info.get(x, {}).get('sim', '') if x != "" else "")
            out['SDT_SIM'] = ktram.apply(lambda x: dict_dc_sdt.get(x, "") if x != "" else "")
            
            def evaluate_md(code):
                if code == "" or code not in s_md: return ""
                
                info = dict_md_info.get(code, {})
                imei = info.get('imei', '')
                serialid = info.get('serial', '')
                metertype = info.get('metertype', '')
                
                is_empty_hardware = (
                    (imei == "" or imei == "NAN") and 
                    (serialid == "" or serialid == "NAN") and 
                    (metertype == "" or metertype == "NAN")
                )
                
                method_ctt = dict_ct_method.get(code, "").upper()
                
                if is_empty_hardware and "RS485" in method_ctt:
                    return "Thu hồi"
                    
                return "MD"
                
            out['MD'] = k13.apply(evaluate_md)
            # Fix: Check nếu ktram != "" trước khi check in s_dc
            out['DCU'] = ktram.apply(lambda x: "DCU" if x != "" and x in s_dc else "")
            
            # Fix: Check nếu k và t != "" trước khi check in s_ct1, s_ct2
            out['CTT'] = [ "CTT" if (k != "" and k in s_ct1) or (t != "" and t in s_ct2) else "" for k, t in zip(k13, ktram) ]
            
            out['STT_MODEM'] = k13.map(dict_modem_full).fillna("")
            out['STT_CTT'] = k13.map(dict_ctt_full).fillna("")
            
            def status(row):
                stt_md = safe_str(row['STT_MODEM'])
                method_ctt = row['METHOD_CTT']
                
                if row['MD'] == "Thu hồi":
                    return f"Thu hồi Modem - Thu thập qua {method_ctt}"
                
                if row['CTT'] == "CTT": 
                    return f"Công Tơ Tổng ({method_ctt})" if method_ctt else "Công Tơ Tổng"
                
                if "CÓ DỮ LIỆU" in stt_md: 
                    return "Modem có dữ liệu"
                
                if row['MD'] == "MD":
                    if row['STT_MODEM'] != "": return f"Modem Offline ({row['STT_MODEM']})"
                    return "Modem Offline"
                    
                if row['DCU'] == "DCU": return "Đo qua DCU"
                
                return "Chưa khai báo"

            out['NHAN_XET'] = out.apply(status, axis=1)
            return out

        tcd_final = process(f_tcd, "TCD")
        tcc_final = process(f_tcc, "TCC")

        if tcd_final is not None and tcc_final is not None:
            sum_tcd, sum_tcc = create_summaries(tcd_final, tcc_final, ma_dvi_filter)
            excel_bytes = to_excel_4_sheets(tcd_final, tcc_final, sum_tcd, sum_tcc)
            st.success("✅ ĐÃ XONG!")
            with st.expander(f"📊 Xem Tổng Hợp (Lọc theo: {ma_dvi_filter if ma_dvi_filter else 'TẤT CẢ'})"): 
                st.dataframe(sum_tcc)
            st.download_button("📥 TẢI KẾT QUẢ V77.xlsx", excel_bytes, "Ket_Qua_V77.xlsx", "primary")
        else:
            st.error("Lỗi xử lý file.")

    except Exception as e: st.error(f"Lỗi: {e}")
