import streamlit as st
import pandas as pd
import io

st.set_page_config(page_title="Đánh Giá Tỷ Lệ Đo Xa (Logic Mới)", layout="wide")

# ================= 1. HÀM HỖ TRỢ =================
def clean_mdd(val):
    if pd.isna(val): return ""
    return str(val).strip().upper()

def check_dcu_data(row, col_ngaygio, col_import):
    """Đánh giá xem điểm đo DCU có dữ liệu hay không dựa vào NGAYGIO và IMPORT"""
    ngaygio = str(row.get(col_ngaygio, "")).strip()
    imp = str(row.get(col_import, "")).strip()
    if ngaygio and ngaygio.lower() != 'nan' and imp and imp.lower() != 'nan':
        return 1
    return 0

def find_column(df, keywords):
    for kw in keywords:
        for col in df.columns:
            if kw.upper() in str(col).upper(): return col
    return None

# ================= 2. GIAO DIỆN TẢI FILE =================
st.title("⚡ Tool Đánh Giá Đo Xa - Phân Tích Cấu Trúc Lưới")

c1, c2, c3 = st.columns(3)
with c1:
    st.markdown("**1. Danh sách Khách Hàng (CMIS)**")
    f_tcc = st.file_uploader("📂 Danh sách TCC", type=['xlsx', 'csv'])
    f_tcd = st.file_uploader("📂 Danh sách TCD", type=['xlsx', 'csv'])
    f_cmis_sautcc = st.file_uploader("📂 File CMIS Khách hàng sau TCC", type=['xlsx', 'csv'])

with c2:
    st.markdown("**2. Nguồn khai thác (DCU)**")
    f_dcu = st.file_uploader("📻 File DCU (PB05, PB06...)", accept_multiple_files=True)

with c3:
    st.markdown("**3. Nguồn khai thác (Modem)**")
    f_md = st.file_uploader("📡 File Modem (EVNHES)", accept_multiple_files=True)

if st.button("🚀 CHẠY PHÂN TÍCH", type="primary"):
    if not (f_tcc and f_tcd and f_dcu and f_md and f_cmis_sautcc):
        st.error("⚠️ Vui lòng tải đầy đủ các file dữ liệu để chạy phân tích!")
        st.stop()

    with st.spinner("Đang bóc tách và đối chiếu dữ liệu..."):
        # --- BƯỚC 1: XÂY DỰNG TỪ ĐIỂN ---
        
        # 1.1 TCD Gốc
        df_tcd_goc = pd.read_excel(f_tcd, dtype=str)
        col_makh_tcd = find_column(df_tcd_goc, ["MA_KHANG", "MA_DDO", "MÃ KHÁCH HÀNG"])
        set_tcd_makh = set(df_tcd_goc[col_makh_tcd].dropna().apply(clean_mdd)) if col_makh_tcd else set()

        # 1.2 DCU
        dict_dcu = {} 
        for f in f_dcu:
            df_d = pd.read_excel(f, dtype=str)
            c_mdd_dcu = find_column(df_d, ["MADIEMDO", "MÃ ĐIỂM ĐO"])
            c_ngaygio = find_column(df_d, ["NGAYGIO", "NGÀY GIỜ", "THỜI GIAN"])
            c_import = find_column(df_d, ["IMPORT"])
            
            if c_mdd_dcu:
                for _, row in df_d.iterrows():
                    mdd = clean_mdd(row[c_mdd_dcu])
                    if mdd:
                        dict_dcu[mdd] = {
                            'nguon': 'DCU',
                            'co_du_lieu': check_dcu_data(row, c_ngaygio, c_import)
                        }

        # 1.3 Modem
        dict_md = {}
        for f in f_md:
            df_m = pd.read_excel(f, dtype=str)
            c_mdd_md = find_column(df_m, ["MADIEMDO", "MÃ ĐIỂM ĐO"])
            c_tt_md = find_column(df_m, ["TRANGTHAI", "TRẠNG THÁI"])
            if c_mdd_md and c_tt_md:
                for _, row in df_m.iterrows():
                    mdd = clean_mdd(row[c_mdd_md])
                    if mdd:
                        status = str(row[c_tt_md]).upper()
                        dict_md[mdd] = {
                            'nguon': 'MD',
                            'co_du_lieu': 1 if "CÓ DỮ LIỆU" in status else 0
                        }

        # 1.4 KH Sau TCC (Lấy số lượng CMIS)
        df_cmis_sau = pd.read_excel(f_cmis_sautcc, dtype=str)
        c_dvi_cmis = find_column(df_cmis_sau, ["MA_DVIQLY", "MÃ ĐƠN VỊ"])
        c_sl_cmis = find_column(df_cmis_sau, ["SO_LUONG", "SỐ LƯỢNG", "CMIS"])
        dict_cmis_sl = dict(zip(df_cmis_sau[c_dvi_cmis].apply(clean_mdd), pd.to_numeric(df_cmis_sau[c_sl_cmis], errors='coerce').fillna(0))) if (c_dvi_cmis and c_sl_cmis) else {}

        # --- BƯỚC 2: QUÉT DIỆN RỘNG NHẬN DIỆN CTT & PHÂN LOẠI TCC ---
        df_tcc = pd.read_excel(f_tcc, dtype=str)
        c_makh_tcc = find_column(df_tcc, ["MA_KHANG", "MA_DDO", "MÃ ĐIỂM ĐO"])
        c_dvi_tcc = find_column(df_tcc, ["MA_DVIQLY"])
        
        # Các cột bổ trợ để dò RS485
        c_method_tcc = find_column(df_tcc, ["METHOD", "PHƯƠNG THỨC", "PHUONG THUC"])
        c_ten_tcc = find_column(df_tcc, ["TEN_KHANG", "TÊN KHÁCH HÀNG", "TEN_DDO", "TÊN ĐIỂM ĐO"])
        
        df_tcc['MA_CHUAN'] = df_tcc[c_makh_tcc].apply(clean_mdd) if c_makh_tcc else ""
        df_tcc['MA_DVIQLY'] = df_tcc[c_dvi_tcc].apply(clean_mdd) if c_dvi_tcc else "UNKNOWN"
        
        def phan_loai_tcc(row):
            ma_val = clean_mdd(row.get(c_makh_tcc, "")) if c_makh_tcc else ""
            method_val = clean_mdd(row.get(c_method_tcc, "")) if c_method_tcc else ""
            ten_val = clean_mdd(row.get(c_ten_tcc, "")) if c_ten_tcc else ""
            
            # Logic quét diện rộng RS485 (kiểm tra cả 3 cột)
            if "RS485" in ma_val or "RS485" in method_val or "RS485" in ten_val:
                return "CTT"
                
            # Cắt mã 13 ký tự để soi với TCD phòng trường hợp mã có đuôi
            ma_13 = ma_val[:13] 
            if ma_val in set_tcd_makh or ma_13 in set_tcd_makh:
                return "TCD"
                
            return "KH_SAU_TCC"
            
        df_tcc['PHAN_LOAI'] = df_tcc.apply(phan_loai_tcc, axis=1)
        
        # Bóc tách DataFrames
        df_kh_sautcc = df_tcc[df_tcc['PHAN_LOAI'] == 'KH_SAU_TCC'].copy()
        df_ctt = df_tcc[df_tcc['PHAN_LOAI'] == 'CTT'].copy()
        df_tcd_bo_sung = df_tcc[df_tcc['PHAN_LOAI'] == 'TCD'].copy()

        # Gom chung TCD
        df_tcd_goc['MA_CHUAN'] = df_tcd_goc[col_makh_tcd].apply(clean_mdd) if col_makh_tcd else ""
        df_tcd_goc['MA_DVIQLY'] = df_tcd_goc[find_column(df_tcd_goc, ["MA_DVIQLY"])].apply(clean_mdd) if find_column(df_tcd_goc, ["MA_DVIQLY"]) else "UNKNOWN"
        df_tcd_tong = pd.concat([df_tcd_goc, df_tcd_bo_sung], ignore_index=True).drop_duplicates(subset=['MA_CHUAN'])

        # --- BƯỚC 3: ĐÁNH GIÁ LUỒNG ĐO XA ---
        def danh_gia_do_xa(df_input):
            if df_input.empty: 
                df_input['NGUON_DOC'] = ""
                df_input['CO_DU_LIEU'] = 0
                return df_input
                
            def check_luong(mdd):
                mdd_13 = mdd[:13] # Hỗ trợ map cả mã gốc lẫn mã cắt 13 ký tự
                if mdd in dict_dcu: return dict_dcu[mdd]['nguon'], dict_dcu[mdd]['co_du_lieu']
                if mdd_13 in dict_dcu: return dict_dcu[mdd_13]['nguon'], dict_dcu[mdd_13]['co_du_lieu']
                
                if mdd in dict_md: return dict_md[mdd]['nguon'], dict_md[mdd]['co_du_lieu']
                if mdd_13 in dict_md: return dict_md[mdd_13]['nguon'], dict_md[mdd_13]['co_du_lieu']
                
                return 'CHƯA ĐO XA', 0
                
            ket_qua = df_input['MA_CHUAN'].apply(check_luong)
            df_input['NGUON_DOC'] = [x[0] for x in ket_qua]
            df_input['CO_DU_LIEU'] = [x[1] for x in ket_qua]
            return df_input

        df_kh_sautcc = danh_gia_do_xa(df_kh_sautcc)
        df_tcd_tong = danh_gia_do_xa(df_tcd_tong)
        df_ctt = danh_gia_do_xa(df_ctt)

        # --- BƯỚC 4: TỔNG HỢP ---
        danh_sach_don_vi = sorted(list(set(df_tcc['MA_DVIQLY'].unique()).union(df_tcd_tong['MA_DVIQLY'].unique())))
        report_data = []

        for dvi in danh_sach_don_vi:
            kh_dv = df_kh_sautcc[df_kh_sautcc['MA_DVIQLY'] == dvi]
            tcd_dv = df_tcd_tong[df_tcd_tong['MA_DVIQLY'] == dvi]
            ctt_dv = df_ctt[df_ctt['MA_DVIQLY'] == dvi]

            # 1. KH Sau TCC
            cmis_sau_tcc = dict_cmis_sl.get(dvi, 0)
            sl_khai_thac_dcu = len(kh_dv[kh_dv['NGUON_DOC'] == 'DCU']) 
            sl_co_du_lieu_sautcc = kh_dv['CO_DU_LIEU'].sum()
            tl_khai_thac = (sl_khai_thac_dcu / cmis_sau_tcc * 100) if cmis_sau_tcc > 0 else 0
            tl_thu_thap_sautcc = (sl_co_du_lieu_sautcc / sl_khai_thac_dcu * 100) if sl_khai_thac_dcu > 0 else 0

            # 2. TCD
            tcd_doc_md = len(tcd_dv[tcd_dv['NGUON_DOC'] == 'MD'])
            tcd_doc_dcu = len(tcd_dv[tcd_dv['NGUON_DOC'] == 'DCU'])
            tcd_tong_khaithac = tcd_doc_md + tcd_doc_dcu
            tcd_co_dulieu = tcd_dv['CO_DU_LIEU'].sum()
            tcd_tyle = (tcd_co_dulieu / tcd_tong_khaithac * 100) if tcd_tong_khaithac > 0 else 0

            # 3. CTT TCC
            ctt_doc_md = len(ctt_dv[ctt_dv['NGUON_DOC'] == 'MD'])
            ctt_doc_dcu = len(ctt_dv[ctt_dv['NGUON_DOC'] == 'DCU'])
            ctt_tong_khaithac = ctt_doc_md + ctt_doc_dcu
            ctt_co_dulieu = ctt_dv['CO_DU_LIEU'].sum()
            ctt_tyle = (ctt_co_dulieu / ctt_tong_khaithac * 100) if ctt_tong_khaithac > 0 else 0

            report_data.append({
                "Mã đơn vị": dvi,
                "SL KH sau TCC (CMIS)": cmis_sau_tcc,
                "SL KH khai thác DCU": sl_khai_thac_dcu,
                "Tỷ lệ khai thác (%)": round(tl_khai_thac, 2),
                "KH DCU có dữ liệu": sl_co_du_lieu_sautcc,
                "Tỷ lệ thu thập KH sau TCC (%)": round(tl_thu_thap_sautcc, 2),
                "TCD - Đọc MD": tcd_doc_md,
                "TCD - Đọc DCU": tcd_doc_dcu,
                "TCD - Đã thu thập": tcd_co_dulieu,
                "Tỷ lệ thu thập TCD (%)": round(tcd_tyle, 2),
                "CTT - Đọc MD": ctt_doc_md,
                "CTT - Đọc DCU": ctt_doc_dcu,
                "CTT - Đã thu thập": ctt_co_dulieu,
                "Tỷ lệ thu thập CTT (%)": round(ctt_tyle, 2)
            })

        df_report = pd.DataFrame(report_data)
        
        # --- XUẤT BÁO CÁO ---
        st.success("✅ Phân tích hoàn tất! Đã bóc tách chính xác CTT theo mọi định dạng.")
        st.dataframe(df_report, use_container_width=True)

        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df_report.to_excel(writer, index=False, sheet_name='TongHop')
            df_kh_sautcc.to_excel(writer, index=False, sheet_name='KH_SauTCC')
            df_tcd_tong.to_excel(writer, index=False, sheet_name='ChiTiet_TCD')
            df_ctt.to_excel(writer, index=False, sheet_name='ChiTiet_CTT')
            
        st.download_button("📥 TẢI KẾT QUẢ PHÂN TÍCH (EXCEL)", data=output.getvalue(), file_name="Bao_Cao_Do_Xa_V2.xlsx", type="primary")
