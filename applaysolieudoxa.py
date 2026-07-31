import streamlit as st
import pandas as pd
import numpy as np
import io

st.set_page_config(page_title="Đánh Giá Đo Xa (Chuẩn TCD -> CTT -> KH)", layout="wide")

# ================= 1. CÁC HÀM XỬ LÝ DỮ LIỆU CỐT LÕI =================

def clean_str(val):
    if pd.isna(val): return ""
    return str(val).strip().upper()

def get_ma_dviqly(mdd):
    mdd = clean_str(mdd)
    if mdd.startswith("PB") and len(mdd) >= 6:
        return mdd[:6]
    return "KHAC"

def check_dcu(row, col_ngaygio, col_import):
    val_ngaygio = row.get(col_ngaygio)
    val_import = row.get(col_import)
    if pd.notna(val_ngaygio) and clean_str(val_ngaygio) != "" and pd.notna(val_import) and clean_str(val_import) != "":
        return 1
    return 0

def check_modem(row, col_trangthai):
    val_stt = clean_str(row.get(col_trangthai))
    if "CÓ DỮ LIỆU" in val_stt or "CO DU LIEU" in val_stt:
        return 1
    return 0

def find_col(df, keywords):
    for kw in keywords:
        for col in df.columns:
            if clean_str(kw) in clean_str(col): return col
    return None

def load_dataframe(file_obj, keywords_to_find_header):
    """Hàm đọc file thế hệ mới: Đọc 1 lần duy nhất, chống kẹt luồng file Streamlit, tìm header trong memory"""
    if file_obj is None: return None
    try:
        is_csv = file_obj.name.lower().endswith('.csv')
        
        # Đọc toàn bộ file 1 lần duy nhất
        if is_csv:
            df = pd.read_csv(file_obj, dtype=str)
        else:
            df = pd.read_excel(file_obj, dtype=str)
            
        # Kịch bản 1: File đã chuẩn, header nằm ngay dòng đầu
        col_str = " ".join([str(x).upper() for x in df.columns])
        if any(clean_str(kw) in col_str for kw in keywords_to_find_header):
            df.columns = [str(c).strip() for c in df.columns]
            return df
            
        # Kịch bản 2: Header bị đẩy xuống dưới, ta duyệt 20 dòng đầu trong DataFrame để tìm
        for i, row in df.head(20).iterrows():
            row_str = " ".join([str(x).upper() for x in row.values if pd.notna(x)])
            if any(clean_str(kw) in row_str for kw in keywords_to_find_header):
                # Gán dòng này làm tên cột mới
                df.columns = [str(c).strip() if pd.notna(c) else f"Unnamed_{j}" for j, c in enumerate(row.values)]
                # Xóa các dòng rác phía trên header
                df = df.iloc[i+1:].reset_index(drop=True)
                return df
                
        # Nếu vẫn không tìm thấy, trả về df mặc định
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Lỗi hệ thống khi đọc file {file_obj.name}: {e}")
        return pd.DataFrame()

# ================= 2. GIAO DIỆN TẢI FILE =================
st.title("⚡ Tool Đánh Giá Đo Xa: Tính Toán Trực Tiếp Từ Nguồn Khai Thác")
st.info("Phiên bản Anti-Crash: Khắc phục lỗi đọc header và fix lỗi cộng chuỗi số liệu CMIS.")

c1, c2, c3 = st.columns(3)
with c1:
    st.markdown("**1. Danh sách CMIS & Quản lý**")
    f_export = st.file_uploader("📂 File Dataexport (TONGCT, NOIBO...)", type=['xlsx', 'csv'])
    f_tcd = st.file_uploader("📂 Danh sách TCD", type=['xlsx', 'csv'])
    f_tcc = st.file_uploader("📂 Danh sách TCC (CTT)", type=['xlsx', 'csv'])

with c2:
    st.markdown("**2. Dữ liệu DCU (Khai thác)**")
    f_dcu = st.file_uploader("📻 Các file PB05/06 DCU...", accept_multiple_files=True)

with c3:
    st.markdown("**3. Dữ liệu Modem (Khai thác)**")
    f_md = st.file_uploader("📡 File EVNHES MODEM...", accept_multiple_files=True)

if st.button("🚀 CHẠY PHÂN TÍCH", type="primary"):
    if not (f_export and f_tcd and f_tcc and f_dcu and f_md):
        st.error("⚠️ Vui lòng tải đầy đủ các file theo 3 nhóm trên!")
        st.stop()

    with st.spinner("Đang phân tích cấu trúc dữ liệu..."):
        
        # --- BƯỚC 1: XÂY DỰNG TẬP HỢP TCD VÀ CTT ---
        df_tcd = load_dataframe(f_tcd, ["MA_KHANG", "MA_DDO", "MÃ KHÁCH HÀNG", "MÃ ĐIỂM ĐO"])
        col_tcd = find_col(df_tcd, ["MA_KHANG", "MA_DDO", "MÃ KHÁCH HÀNG", "MÃ ĐIỂM ĐO"])
        set_tcd = set(df_tcd[col_tcd].apply(clean_str).dropna()) if col_tcd else set()

        df_tcc = load_dataframe(f_tcc, ["MA_KHANG", "MA_DDO", "MÃ KHÁCH HÀNG", "MÃ ĐIỂM ĐO"])
        col_tcc = find_col(df_tcc, ["MA_KHANG", "MA_DDO", "MÃ KHÁCH HÀNG", "MÃ ĐIỂM ĐO"])
        set_ctt = set()
        if col_tcc:
            for mdd in df_tcc[col_tcc].dropna():
                m = clean_str(mdd)
                if m not in set_tcd:
                    set_ctt.add(m)

        def phan_loai_diem_do(mdd):
            if "RS485" in mdd: return 'CTT'
            if mdd in set_tcd: return 'TCD'
            if mdd in set_ctt: return 'CTT'
            return 'KH_SAU_TCC'

        # --- BƯỚC 2: ĐỌC DATAEXPORT CMIS ĐỂ TÍNH BASELINE ---
        df_export = load_dataframe(f_export, ["MA_DONVI", "MA_DVIQLY", "Mã đơn vị", "MA_DVI"])
        
        # Thoát nếu DF lỗi đọc trống
        if df_export is None or df_export.empty:
            st.error("❌ Không thể nạp được dữ liệu từ file DataExport. Vui lòng thử lưu lại file thành định dạng .xlsx chuẩn.")
            st.stop()
            
        c_dvi_exp = find_col(df_export, ["MA_DONVI", "MA_DVIQLY", "Mã đơn vị", "MA_DVI"])
        c_tong_ct = find_col(df_export, ["TONGCT", "TỔNG CÔNG TƠ", "TỔNG CT"])
        
        if not c_dvi_exp or not c_tong_ct:
            st.error("❌ Lỗi cấu trúc: Không tìm thấy cột 'MA_DONVI' hoặc 'TONGCT' trong file DataExport.")
            st.stop()
            
        c_noibo_1p = find_col(df_export, ["NOIBO_1P", "NỘI BỘ 1P"]) or 'NOIBO_1P_TEMP'
        c_noibo_3p = find_col(df_export, ["NOIBO_3P", "NỘI BỘ 3P"]) or 'NOIBO_3P_TEMP'
        
        if c_noibo_1p == 'NOIBO_1P_TEMP': df_export['NOIBO_1P_TEMP'] = 0
        if c_noibo_3p == 'NOIBO_3P_TEMP': df_export['NOIBO_3P_TEMP'] = 0
        
        # QUAN TRỌNG: Ép kiểu dữ liệu về Numeric (Số) để tránh lỗi dồn chuỗi chữ gây ra số âm
        df_export[c_tong_ct] = pd.to_numeric(df_export[c_tong_ct], errors='coerce').fillna(0)
        df_export[c_noibo_1p] = pd.to_numeric(df_export[c_noibo_1p], errors='coerce').fillna(0)
        df_export[c_noibo_3p] = pd.to_numeric(df_export[c_noibo_3p], errors='coerce').fillna(0)
                
        export_agg = df_export.groupby(c_dvi_exp).agg(
            TONGCT=(c_tong_ct, 'sum'),
            NOIBO_1P=(c_noibo_1p, 'sum'),
            NOIBO_3P=(c_noibo_3p, 'sum')
        ).reset_index().rename(columns={c_dvi_exp: 'MA_DVIQLY'})
        
        df_tcd['MA_DVIQLY'] = df_tcd[col_tcd].apply(get_ma_dviqly) if col_tcd else "KHAC"
        tcd_cmis_counts = df_tcd.groupby('MA_DVIQLY').size().reset_index(name='SL_TCD_CMIS')
        
        cmis_base = pd.merge(export_agg, tcd_cmis_counts, on='MA_DVIQLY', how='left').fillna(0)
        
        # Đảm bảo phép tính trừ giờ đây luôn là 100% chuẩn xác
        cmis_base['SL_KH_SAU_TCC_CMIS'] = cmis_base['TONGCT'] - cmis_base['SL_TCD_CMIS'] - cmis_base['NOIBO_1P'] - cmis_base['NOIBO_3P']

        # --- BƯỚC 3: QUÉT TOÀN BỘ DỮ LIỆU ĐANG KHAI THÁC TRÊN DCU VÀ MODEM ---
        khai_thac_data = []

        # 3.1 Quét DCU
        for f in f_dcu:
            df_d = load_dataframe(f, ["ASSETID", "MA_DIEMDO", "MADIEMDO", "MÃ ĐIỂM ĐO"])
            c_mdd = find_col(df_d, ["ASSETID", "MA_DIEMDO", "MADIEMDO", "MÃ ĐIỂM ĐO"])
            c_ngay = find_col(df_d, ["NGAYGIO", "NGÀY GIỜ", "THỜI GIAN"])
            c_imp = find_col(df_d, ["IMPORTKWH", "IMPORT"])
            
            if c_mdd:
                for _, row in df_d.iterrows():
                    mdd = clean_str(row[c_mdd])
                    if mdd:
                        khai_thac_data.append({
                            'MA_DVIQLY': get_ma_dviqly(mdd),
                            'MADIEMDO': mdd,
                            'PHAN_LOAI': phan_loai_diem_do(mdd),
                            'NGUON_DOC': 'DCU',
                            'CO_DU_LIEU': check_dcu(row, c_ngay, c_imp)
                        })

        # 3.2 Quét Modem
        for f in f_md:
            df_m = load_dataframe(f, ["MA_DIEMDO", "MADIEMDO", "MÃ ĐIỂM ĐO"])
            c_mdd = find_col(df_m, ["MA_DIEMDO", "MADIEMDO", "MÃ ĐIỂM ĐO"])
            c_stt = find_col(df_m, ["TINHTRANG", "TRANGTHAI", "TRẠNG THÁI", "TÌNH TRẠNG"])
            
            if c_mdd:
                for _, row in df_m.iterrows():
                    mdd = clean_str(row[c_mdd])
                    if mdd:
                        khai_thac_data.append({
                            'MA_DVIQLY': get_ma_dviqly(mdd),
                            'MADIEMDO': mdd,
                            'PHAN_LOAI': phan_loai_diem_do(mdd),
                            'NGUON_DOC': 'MD',
                            'CO_DU_LIEU': check_modem(row, c_stt)
                        })

        df_all = pd.DataFrame(khai_thac_data)
        
        # --- BƯỚC 4: TỔNG HỢP VÀ TÍNH TỶ LỆ ---
        if df_all.empty:
            st.error("Không tìm thấy dữ liệu hợp lệ từ các file DCU và Modem tải lên.")
            st.stop()
            
        danh_sach_dv = sorted(df_all['MA_DVIQLY'].unique())
        report_data = []

        for dvi in danh_sach_dv:
            dv_data = df_all[df_all['MA_DVIQLY'] == dvi]
            
            cmis_info = cmis_base[cmis_base['MA_DVIQLY'] == dvi]
            sl_kh_cmis = cmis_info['SL_KH_SAU_TCC_CMIS'].values[0] if not cmis_info.empty else 0
            
            # KHÁCH HÀNG SAU TCC
            kh = dv_data[dv_data['PHAN_LOAI'] == 'KH_SAU_TCC']
            kh_dcu = len(kh[kh['NGUON_DOC'] == 'DCU'])
            kh_md = len(kh[kh['NGUON_DOC'] == 'MD'])
            kh_co_data = kh['CO_DU_LIEU'].sum()
            kh_khaithac = kh_dcu + kh_md
            tl_khaithac = (kh_khaithac / sl_kh_cmis * 100) if sl_kh_cmis > 0 else 0
            tl_thuthap = (kh_co_data / kh_khaithac * 100) if kh_khaithac > 0 else 0
            
            # TCD
            tcd = dv_data[dv_data['PHAN_LOAI'] == 'TCD']
            tcd_dcu = len(tcd[tcd['NGUON_DOC'] == 'DCU'])
            tcd_md = len(tcd[tcd['NGUON_DOC'] == 'MD'])
            tcd_co_data = tcd['CO_DU_LIEU'].sum()
            tcd_khaithac = tcd_dcu + tcd_md
            tl_tcd = (tcd_co_data / tcd_khaithac * 100) if tcd_khaithac > 0 else 0
            
            # CTT
            ctt = dv_data[dv_data['PHAN_LOAI'] == 'CTT']
            ctt_dcu = len(ctt[ctt['NGUON_DOC'] == 'DCU'])
            ctt_md = len(ctt[ctt['NGUON_DOC'] == 'MD'])
            ctt_co_data = ctt['CO_DU_LIEU'].sum()
            ctt_khaithac = ctt_dcu + ctt_md
            tl_ctt = (ctt_co_data / ctt_khaithac * 100) if ctt_khaithac > 0 else 0
            
            report_data.append({
                "Mã đơn vị": dvi,
                "SL KH sau TCC CMIS": int(sl_kh_cmis),
                "KH sau TCC đang khai thác": kh_khaithac,
                "Chênh lệch": int(sl_kh_cmis) - kh_khaithac,
                "Tỷ lệ khai thác KH (%)": round(tl_khaithac, 2),
                "KH sau TCC có dữ liệu": kh_co_data,
                "Tỷ lệ thu thập KH sau TCC (%)": round(tl_thuthap, 2),
                "TCD Khai thác MD": tcd_md,
                "TCD Khai thác DCU": tcd_dcu,
                "TCD Có dữ liệu": tcd_co_data,
                "Tỷ lệ thu thập TCD (%)": round(tl_tcd, 2),
                "CTT Khai thác MD": ctt_md,
                "CTT Khai thác DCU": ctt_dcu,
                "CTT Có dữ liệu": ctt_co_data,
                "Tỷ lệ thu thập CTT (%)": round(tl_ctt, 2)
            })

        df_report = pd.DataFrame(report_data)

        # --- BƯỚC 5: XUẤT BÁO CÁO PHÂN TÁCH ---
        st.success("✅ Phân tích hoàn tất! Dữ liệu đã được tính chuẩn xác, bảng siêu lớn > 1 triệu dòng đã được tách thành file CSV riêng biệt.")
        st.dataframe(df_report, use_container_width=True)

        output_excel = io.BytesIO()
        with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
            df_report.to_excel(writer, index=False, sheet_name='TongHop')
        
        output_csv = df_all.to_csv(index=False).encode('utf-8')
        
        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            st.download_button("📥 1. TẢI BÁO CÁO TỔNG HỢP (EXCEL)", data=output_excel.getvalue(), file_name="Bao_Cao_Do_Xa.xlsx", type="primary")
        with col_dl2:
            st.download_button("📥 2. TẢI DATA CHI TIẾT ĐIỂM ĐO (CSV)", data=output_csv, file_name="Chi_Tiet_1_Trieu_Diem_Do.csv")
