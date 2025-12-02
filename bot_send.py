import os
import warnings 
warnings.filterwarnings("ignore")
from vnstock import Vnstock, Quote, Listing
import pandas as pd
from datetime import datetime
import datetime as dt
from dateutil.relativedelta import relativedelta
import time
import requests
import pytz 

stock = Vnstock().stock(symbol='FPT', source='VCI')

# --- CẤU HÌNH (LẤY TỪ GITHUB SECRETS) ---
# Nếu chạy trên máy cá nhân thì điền trực tiếp, lên Github thì nó tự lấy từ biến môi trường
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN') 
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')

VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')
time_sleep = 60
# List mã của bạn
full = [
    "AAA", "ACB", "AGR", "ANV", "ASM", "BAF", "BCG", "BID", "BSI", "BSR", "CII",
    "CRE", "CSM", "CTG", "CTI", "CTS", "DBC", "DCM", "DGC", "DGW", "DIG", "DLG",
    "DPG", "DPM", "DSE", "DXG", "DXS", "EIB", "EVF", "EVG", "FCN", "FPT", "FTS",
    "GEX", "GMD", "GVR", "HAG", "HAH", "HCM", "HDB", "HDC", "HDG", "HHS", "HHV",
    "HPG", "HPX", "HQC", "HSG", "HTN", "HVN", "IDI", "IJC", "KBC", "KDH", "KHG",
    "KSB", "LCG", "LDG", "LPB", "MBB", "MSB", "MSN", "MWG", "NAB", "NKG", "NLG",
    "NT2", "NTL", "NVL", "OCB", "ORS", "PAN", "PC1", "PDR", "PET", "PLX", "POW",
    "PVD", "PVT", "QCG", "SCR", "SHB", "SSB", "SSI", "STB", "SZC", "TCB", "TCH",
    "TCM", "TPB", "TTF", "VCB", "VCG", "VCI", "VDS", "VGC", "VHC", "VHM", "VIB",
    "VIC", "VIX", "VJC", "VND", "VNM", "VOS", "VPB", "VPI", "VRE", "VSC", "YEG", 'VNINDEX'
]

vn100 = Listing().symbols_by_group('VN100').tolist()
cp = list(set(vn100) | set(full))

alert_tracker = {} 

# --- HÀM GỬI TELEGRAM ---
def send_telegram(message):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print("❌ Chưa cấu hình Token Telegram trong Secrets!")
        return
        
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}
    while True:
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200: break
            elif response.status_code == 429: time.sleep(5)
            else: time.sleep(5)
        except: time.sleep(5)

# --- CÁC HÀM XỬ LÝ (GIỮ NGUYÊN LOGIC CŨ CỦA BẠN) ---
# Bạn dán phần logic notification, download_data mà tôi đã đưa ở câu trả lời trước vào đây nhé.
# Để ngắn gọn tôi viết tóm tắt cấu trúc, bạn nhớ paste full code vào.

def notification(ticker, df_day, df_minute, price_threshold=None, vol_multiplier=1.5):
    # ==============================================================================
    # BƯỚC 0: KIỂM TRA QUOTA CẢNH BÁO (LOGIC MỚI CỦA SẾP)
    # ==============================================================================
    global alert_tracker
    
    # Khởi tạo tracker cho mã này nếu chưa có
    if ticker not in alert_tracker:
        alert_tracker[ticker] = {'AM': 0, 'PM': 0}
    
    # Xác định phiên hiện tại (Sau 13:00 là phiên chiều PM)
    current_hour = datetime.now().hour
    session = 'PM' if current_hour >= 13 else 'AM'
    
    # *** LOGIC TỐI ƯU ***: Nếu đã báo đủ 2 lần trong phiên -> DỪNG NGAY
    if alert_tracker[ticker][session] >= 2:
        # print(f"-> {ticker} đã báo đủ 2 lần phiên {session}. Bỏ qua để tiết kiệm tài nguyên.")
        return 

    # ==============================================================================
    # BƯỚC 1: KHỞI TẠO BASELINE (GIỮ NGUYÊN)
    # ==============================================================================  
    try:
        df_day_ticker = df_day.loc[df_day['ticker'] == ticker]
        df_minute_ticker = df_minute.loc[df_minute['ticker'] == ticker].copy() # Copy để tránh warning
        
        if df_minute_ticker.empty:
            return

        df_minute_ticker.reset_index(inplace=True)
        
        if price_threshold is None:
            # Lấy giá trị close dòng cuối cùng
            if not df_day_ticker.empty:
                price_threshold = round(float(df_day_ticker['Close'].rolling(20).mean().iloc[-1]), 2)
            else:
                price_threshold = 0 # Fallback nếu không có data day

        # ==============================================================================
        # BƯỚC 1.5: LẤY DATA INTRADAY (CHỈ LẤY KHI CHƯA HẾT QUOTA)
        # ==============================================================================
        df_intraday = Quote(symbol=ticker, source='VCI').intraday(symbol=ticker, page_size=100_000)
        
        if df_intraday is None or df_intraday.empty:
            return

        # --- Xử lý data Minute History (Baseline) ---
        df_minute_ticker['Time'] = pd.to_datetime(df_minute_ticker['Time'])
        df_minute_ticker['date'] = df_minute_ticker['Time'].dt.date
        
        unique_dates = sorted(df_minute_ticker['date'].unique())
        recent_dates = unique_dates[-20:]
        
        daily_filled_data = []
        standard_time_range = pd.date_range(start="09:15", end="14:30", freq="1min").time
        
        for d in recent_dates:
            day_df = df_minute_ticker[df_minute_ticker['date'] == d].copy()
            day_df = day_df.set_index('Time')
            full_index = [datetime.combine(d, t) for t in standard_time_range]
            day_df_full = day_df.reindex(full_index)
            day_df_full['Volume'] = day_df_full['Volume'].fillna(0)
            day_df_full['cum_vol'] = day_df_full['Volume'].cumsum()
            day_df_full['time_str'] = day_df_full.index.strftime('%H:%M')
            daily_filled_data.append(day_df_full)
        
        if not daily_filled_data:
            return
            
        history_full = pd.concat(daily_filled_data)
        baseline_map = history_full.groupby('time_str')['cum_vol'].mean().to_dict()

        # ==============================================================================
        # BƯỚC 2: QUÉT INTRADAY
        # ==============================================================================
        
        df_intraday['time'] = pd.to_datetime(df_intraday['time'])
        if df_intraday['time'].dt.tz is not None:
            df_intraday['time'] = df_intraday['time'].dt.tz_localize(None)
            
        # Gộp Intraday về từng phút
        df_today_min = df_intraday.set_index('time').resample('1min').agg({
            'price': 'last',  
            'volume': 'sum'   
        }).dropna()
        
        df_today_min['cum_vol_today'] = df_today_min['volume'].cumsum()
        
        # --- VÒNG LẶP CHECK ---
        # Chỉ check những phút mới nhất hoặc check toàn bộ (ở đây code check toàn bộ intraday đã có)
        # Để tránh spam tin nhắn cũ, ta sẽ check từ dưới lên hoặc check hết nhưng lọc bằng alert_tracker
        
        for t, row in df_today_min.iterrows():
            # Check lại quota lần nữa (vì trong vòng lặp này có thể nó báo đủ 2 lần rồi)
            if alert_tracker[ticker][session] >= 2:
                break 

            time_key = t.strftime('%H:%M')
            
            # Lấy dữ liệu
            price_now = row['price']
            vol_now = row['cum_vol_today']
            vol_ma20 = baseline_map.get(time_key, 0)
            
            # Logic so sánh
            is_vol_spike = False
            ratio = 0
            
            if vol_ma20 > 0:
                ratio = vol_now / vol_ma20
                if ratio >= vol_multiplier:
                    is_vol_spike = True
            
            is_price_break = price_now > price_threshold
            
            # 4. LOGIC CẢNH BÁO & GỬI TELEGRAM
            if is_vol_spike or is_price_break:
                
                # Tạo nội dung tin nhắn
                msg = ""
                send_signal = False # Cờ để quyết định có gửi hay không

                if is_vol_spike and is_price_break:
                    msg = (f"🔥🔥🔥 <b>{ticker}</b> | {time_key}\n"
                           f"<b>SUPER ALERT: GIÁ VÀ VOL ĐỀU NỔ!</b>\n"
                           f"💰 Giá: {price_now} (> {price_threshold})\n"
                           f"🚀 Vol tích lũy: {vol_now:,.0f} (x{ratio:.1f} MA20)")
                    send_signal = True
                
                elif is_vol_spike:
                    msg = (f"🚀 <b>{ticker}</b> | {time_key}\n"
                           f"<b>CẢNH BÁO VOL: Nổ Volume (x{ratio:.1f})</b>\n"
                           f"📊 Vol: {vol_now:,.0f} vs MA20: {vol_ma20:,.0f}\n"
                           f"💵 Giá: {price_now}")
                    send_signal = True
                    
                elif is_price_break:
                    msg = (f"🔔 <b>{ticker}</b> | {time_key}\n"
                           f"<b>CẢNH BÁO GIÁ: Vượt ngưỡng {price_threshold}</b>\n"
                           f"💵 Giá hiện tại: {price_now}\n"
                           f"📊 Vol ratio: {ratio:.1f}x")
                    send_signal = True
                
                # Gửi Telegram và Tăng biến đếm
                if send_signal:
                    print(f"Detect {ticker} at {time_key}. Sending Telegram...") # Log ra console để biết
                    send_telegram(msg)
                    
                    # Tăng biến đếm quota
                    alert_tracker[ticker][session] += 1
                    
    except Exception as e:
        print(f"Error processing {ticker}: {e}")

def get_stock_price(tickers, type='long', start=None, end=None, interval='1D', time_sleep=60):
    if start is None:
        start = '2000-01-01'
    if end is None:
        end = str(datetime.date.today())
    
    if isinstance(tickers, str):
        if tickers == 'VN30':
            cp = Listing().symbols_by_group('VN30').tolist() + ['VNINDEX']
        if tickers == 'VN100':
            cp = Listing().symbols_by_group('VN100').tolist() + ['VNINDEX']  
        if tickers == 'full':
            vn100 = Listing().symbols_by_group('VN100').tolist()
            cp = list(set(vn100) | set(full))
    elif isinstance(tickers, list):
        cp = tickers + ['VNINDEX']

    df = pd.DataFrame()
    parts = []
    for idx, symbol in enumerate(cp, start=1):
        a = stock.quote.history(symbol=symbol, start=start, end=end, interval=interval)
        a.columns = [col.capitalize() for col in a.columns]
        a['ticker'] = symbol
        parts.append(a)
        if idx % 50 == 0:
            print(f"Đã lấy {idx} mã, tạm nghỉ {time_sleep} giây để tránh giới hạn request...")
            time.sleep(time_sleep)
    df = pd.concat(parts, ignore_index=True).set_index('Time')
    if type == 'width':
        df = df.pivot_table(index=df.index, columns='ticker')
        df.columns = df.columns.swaplevel(0, 1)
        df.columns.names = ['ticker', 'attribute']

    return df

def download_data():
    print("📥 Đang tải dữ liệu lịch sử (History Day/Minute)...")
    start = str(dt.date.today() - relativedelta(months=2))
    end = str(dt.date.today() - dt.timedelta(days=1))
    df_minute = get_stock_price(tickers=cp, start=start, end=end, interval='1m', time_sleep=60)
    df_day = get_stock_price(tickers=cp, start=start, end=end, interval='1D', time_sleep=60)

    print("✅ Đã tải xong dữ liệu!")
    # Return dummy data (Thay bằng df thật)
    return df_minute, df_day

def main():
    # 1. TẢI DATA NGAY KHI BẬT MÁY (Lúc 8:30 hoặc 12:30)
    df_minute_hist, df_day_hist = download_data()
    
    print("⏳ Bot đang chạy. Kiểm tra giờ để vào việc...")
    
    while True:
        now = datetime.now(VN_TZ)
        current_hm = now.hour * 100 + now.minute # VD: 915, 1300
        
        # --- XÁC ĐỊNH KHUNG GIỜ ---
        
        # CASE 1: CHỜ SÁNG (08:30 -> 09:15)
        if 830 <= current_hm < 915:
            print(f"[{now.strftime('%H:%M')}] Chờ phiên sáng (09:15)...", end='\r')
            time.sleep(30) # Ngủ 30s check lại
            
        # CASE 2: CHIẾN ĐẤU SÁNG (09:15 -> 11:30)
        elif 915 <= current_hm < 1130:
            # Quét liên tục
            for idx, symbol in enumerate(cp, start=1):
                # Gọi hàm notification
                notification(symbol, df_day_hist, df_minute_hist, price_threshold=1000)
                
                if idx % 50 == 0:
                    print(f"Đã lấy {idx} mã, tạm nghỉ {time_sleep} giây để tránh giới hạn request...")
                    time.sleep(time_sleep)
            
        # CASE 3: HẾT GIỜ SÁNG (>= 11:30 và < 12:00) -> TẮT MÁY
        elif 1130 <= current_hm < 1200:
            print("\n🛑 Hết phiên sáng (11:30). Bot tắt để tiết kiệm GitHub Action.")
            break # Thoát script -> Action Done.

        # CASE 4: CHỜ CHIỀU (12:30 -> 13:00)
        elif 1230 <= current_hm < 1300:
            print(f"[{now.strftime('%H:%M')}] Chờ phiên chiều (13:00)...", end='\r')
            time.sleep(30)
            
        # CASE 5: CHIẾN ĐẤU CHIỀU (13:00 -> 15:00)
        elif 1300 <= current_hm < 1500:
            for idx, symbol in enumerate(cp, start=1):
                # Gọi hàm notification
                notification(symbol, df_day_hist, df_minute_hist, price_threshold=1000)
                
                if idx % 50 == 0:
                    print(f"Đã lấy {idx} mã, tạm nghỉ {time_sleep} giây để tránh giới hạn request...")
                    time.sleep(time_sleep)

        # CASE 6: HẾT GIỜ CHIỀU (>= 15:00) -> TẮT MÁY
        elif current_hm >= 1500:
            print("\n🏁 Hết phiên chiều (15:00). Bot tắt. Hẹn gặp lại mai!")
            break # Thoát script
            
        # CASE NGOẠI LỆ (Nếu lỡ bật sai giờ)
        else:
            print(f"[{now.strftime('%H:%M')}] Giờ không hợp lệ. Bot tắt.")
            break

if __name__ == "__main__":
    # Logic download và chạy main
    # Lưu ý: Code main() ở câu trả lời trước tôi đã viết sẵn rồi
    # Bạn chỉ cần copy paste vào là chạy.
    # Nhớ thêm dòng này để test biến môi trường
    print(f"Bot khởi động với Token ID: ...{str(TELEGRAM_TOKEN)[-5:] if TELEGRAM_TOKEN else 'None'}")
    
    # Giả lập main loop của câu trước
    # main()