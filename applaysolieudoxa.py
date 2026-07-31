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
    if file_obj is None: return None
    try:
        file_obj.seek(0)
        is_csv = file_obj.name.lower().endswith('.csv')
        
        if is_csv:
            df_preview = pd.read_csv(file_obj, header=None, nrows=20, dtype=str)
        else:
            df_preview = pd.read_excel(file_obj, header=None, nrows=20, dtype=str)
            
        header_idx = 0
        for i, row in df_preview.iterrows():
            row_str = " ".join([str(x).upper() for x in row.values if pd.notna(x)])
            if any(clean_str(kw) in row_str for kw in keywords_to_find_header):
                header_idx = i
                break
                
        file_obj.seek(0)
        if is_csv:
            df = pd.read_csv(file_obj, header=header_idx)
        else:
            df = pd.read_excel(file_obj, header=header_idx)
            
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception as e:
        st.error(f"Lỗi đọc file {file_obj.name}: {e}")
        return pd.DataFrame()

# ================= 2. GIAO DIỆN TẢI FILE =================
st.title("⚡ Tool Đánh Giá Đo Xa: Tính Toán Trực Tiếp Từ Nguồn Khai Thác")
st.info("Phiên bản hỗ trợ xử lý dữ liệu siêu lớn (> 1 triệu công tơ)")

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

    with st.spinner("Đang xây dựng ma trận phân tích và xử lý dữ liệu..."):
        
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
        c_dvi_exp = find_col(df_export, ["MA_DONVI", "MA_DVIQLY", "Mã đơn vị", "MA_DVI"])
        c_tong_ct = find_col(df_export, ["TONGCT", "TỔNG CÔNG TƠ", "TỔNG CT"])
        
        # Cảnh báo chặn lỗi tính ra số âm nếu file bị sai cấu trúc
        if not c_dvi_exp or not c_tong_ct:
            st.error("❌ Lỗi cấu trúc: Không tìm thấy cột 'MA_DONVI' hoặc 'TONGCT' trong file DataExport. Vui lòng kiểm tra lại file!")
            st.stop()
            
        c_noibo_1p = find_col(df_export, ["NOIBO_1P", "NỘI BỘ 1P"]) or 'NOIBO_1P_TEMP'
        c_noibo_3p = find_col(df_export, ["NOIBO_3P", "NỘI BỘ 3P"]) or 'NOIBO_3P_TEMP'
        
        if c_noibo_1p == 'NOIBO_1P_TEMP': df_export['NOIBO_1P_TEMP'] = 0
        if c_noibo_3p == 'NOIBO_3P_TEMP': df_export['NOIBO_3P_TEMP'] = 0
                
        export_agg = df_export.groupby(c_dvi_exp).agg(
            TONGCT=(c_tong_ct, 'sum'),
            NOIBO_1P=(c_noibo_1p, 'sum'),
            NOIBO_3P=(c_noibo_3p, 'sum')
        ).reset_index().rename(columns={c_dvi_exp: 'MA_DVIQLY'})
        
        df_tcd['MA_DVIQLY'] = df_tcd[col_tcd].apply(get_ma_dviqly) if col_tcd else "KHAC"
        tcd_cmis_counts = df_tcd.groupby('MA_DVIQLY').size().reset_index(name='SL_TCD_CMIS')
        
        cmis_base = pd.merge(export_agg, tcd_cmis_counts, on='MA_DVIQLY', how='left').fillna(0)
        # Công thức: Khách hàng sau TCC = Tổng công tơ - TCD - Nội bộ (TCC)
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

        # --- BƯỚC 5: XUẤT BÁO CÁO PHÂN TÁCH ĐỂ TRÁNH QUÁ TẢI EXCEL ---
        st.success("✅ Phân tích hoàn tất! Bảng dữ liệu chi tiết vượt quá 1 triệu dòng đã được tách riêng.")
        st.dataframe(df_report, use_container_width=True)

        # 1. Xuất file Báo Cáo Tổng Hợp (Excel)
        output_excel = io.BytesIO()
        with pd.ExcelWriter(output_excel, engine='xlsxwriter') as writer:
            df_report.to_excel(writer, index=False, sheet_name='TongHop')
        
        # 2. Xuất file Dữ Liệu Chi Tiết (CSV để không bị lỗi 1 triệu dòng)
        output_csv = df_all.to_csv(index=False).encode('utf-8')
        
        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            st.download_button("📥 1. TẢI BÁO CÁO TỔNG HỢP (EXCEL)", data=output_excel.getvalue(), file_name="Bao_Cao_Do_Xa.xlsx", type="primary")
        with col_dl2:
            st.download_button("📥 2. TẢI DATA CHI TIẾT (CSV)", data=output_csv, file_name="Chi_Tiet_1_Trieu_Diem_Do.csv")
