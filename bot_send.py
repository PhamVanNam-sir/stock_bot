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

# --- CẤU HÌNH ---
stock = Vnstock().stock(symbol='FPT', source='VCI')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN') 
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
VN_TZ = pytz.timezone('Asia/Ho_Chi_Minh')

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
            elif response.status_code == 429: time.sleep(30) # Bị chặn spam thì nghỉ lâu chút
            else: time.sleep(5)
        except: time.sleep(5)

# --- HÀM LẤY DATA INTRADAY & CHECK LOGIC ---
def notification(ticker, df_day, df_minute, price_threshold=None, vol_multiplier=1.5):
    global alert_tracker
    
    # 1. CHECK QUOTA (QUAN TRỌNG: Nằm đầu tiên để tiết kiệm tài nguyên)
    if ticker not in alert_tracker:
        alert_tracker[ticker] = {'AM': 0, 'PM': 0}
    
    current_hour = datetime.now(VN_TZ).hour
    session = 'PM' if current_hour >= 13 else 'AM'
    
    # Nếu đã đủ 2 lần -> RETURN NGAY (Không chạy các dòng lệnh dưới)
    if alert_tracker[ticker][session] >= 2:
        return 

    # 2. XỬ LÝ DATA
    try:
        # Lấy baseline từ history đã tải sẵn
        df_minute_ticker = df_minute.loc[df_minute['ticker'] == ticker].copy() if 'ticker' in df_minute.columns else pd.DataFrame()
        if df_minute_ticker.empty: return

        df_day_ticker = df_day.loc[df_day['ticker'] == ticker] if 'ticker' in df_day.columns else pd.DataFrame()
        
        # Xác định ngưỡng giá
        if price_threshold is None:
            if not df_day_ticker.empty:
                price_threshold = round(float(df_day_ticker['Close'].rolling(20).mean().iloc[-1]), 2)
            else: price_threshold = 0

        # --- LẤY INTRADAY (Chỉ chạy khi chưa hết quota) ---
        # Hàm này tốn request, nên phải đặt sau check quota
        try:
            df_intraday = Quote(symbol=ticker, source='VCI').intraday(symbol=ticker, page_size=100_000)
        except: return
            
        if df_intraday is None or df_intraday.empty: return

        # ... (Đoạn xử lý Baseline giữ nguyên logic cũ) ...
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
        
        if not daily_filled_data: return
        history_full = pd.concat(daily_filled_data)
        baseline_map = history_full.groupby('time_str')['cum_vol'].mean().to_dict()

        # ... (Đoạn xử lý Intraday giữ nguyên logic cũ) ...
        df_intraday['time'] = pd.to_datetime(df_intraday['time'])
        if df_intraday['time'].dt.tz is not None:
            df_intraday['time'] = df_intraday['time'].dt.tz_localize(None)
            
        df_today_min = df_intraday.set_index('time').resample('1min').agg({
            'price': 'last', 'volume': 'sum'   
        }).dropna()
        df_today_min['cum_vol_today'] = df_today_min['volume'].cumsum()
        
        if df_today_min.empty: return
        
        # Check nến phút cuối cùng
        last_time = df_today_min.index[-1]
        last_row = df_today_min.iloc[-1]
        time_key = last_time.strftime('%H:%M')
        price_now = last_row['price']
        vol_now = last_row['cum_vol_today']
        vol_ma20 = baseline_map.get(time_key, 0)
        
        is_vol_spike = False
        ratio = 0
        if vol_ma20 > 0:
            ratio = vol_now / vol_ma20
            if ratio >= vol_multiplier: is_vol_spike = True
        
        is_price_break = price_now > price_threshold
        
        # GỬI TIN
        if is_vol_spike or is_price_break:
            # Tăng Quota
            alert_tracker[ticker][session] += 1
            
            # Gửi Telegram
            today_hashtag = f"#{datetime.now(VN_TZ).strftime('%d-%m-%Y')}"
            msg = ""
            if is_vol_spike and is_price_break:
                msg = (f"🔥🔥🔥 <b>{ticker}</b> | {time_key}\n"
                           f"<b>SUPER ALERT: GIÁ VÀ VOL ĐỀU NỔ!</b>\n"
                           f"💰 Giá: {price_now} (> {price_threshold})\n"
                           f"🚀 Vol tích lũy: {vol_now:,.0f} (x{ratio:.1f} MA20)")
            elif is_vol_spike:
                msg = (f"🚀 <b>{ticker}</b> | {time_key}\n"
                           f"<b>CẢNH BÁO VOL: Nổ Volume (x{ratio:.1f})</b>\n"
                           f"📊 Vol: {vol_now:,.0f} vs MA20: {vol_ma20:,.0f}\n"
                           f"💵 Giá hiện tại: {price_now}")
            elif is_price_break:
                msg = (f"🔔 <b>{ticker}</b> | {time_key}\n"
                           f"<b>CẢNH BÁO GIÁ: Vượt ngưỡng {price_threshold}</b>\n"
                           f"💵 Giá hiện tại: {price_now}\n"
                           f"📊 Vol ratio: {ratio:.1f}x")
            
            print(f"✅ {ticker}: Có biến ({alert_tracker[ticker][session]}/2) -> Gửi tin...")
            send_telegram(msg)

    except Exception as e:
        # print(f"Error {ticker}: {e}")
        return

# --- HÀM TẢI DATA LỊCH SỬ (Chạy 1 lần đầu giờ) ---
def get_stock_price(tickers, start=None, end=None, interval='1D', time_sleep=60):
    if start is None: start = '2000-01-01'
    if end is None: end = str(datetime.date.today())
    
    # Xử lý list ticker
    # (Giữ nguyên logic của bạn, chỉ rút gọn cho dễ nhìn)
    if isinstance(tickers, str):
        if tickers == 'full':
            vn100 = Listing().symbols_by_group('VN100').tolist()
            cp_list = list(set(vn100) | set(full))
        else: cp_list = [tickers]
    else: cp_list = tickers

    print(f"📥 Đang tải dữ liệu {interval} cho {len(cp_list)} mã...")
    parts = []
    # Tải data history không cần quá nhanh, cứ tà tà để không bị chặn
    for idx, symbol in enumerate(cp_list, start=1):
        try:
            a = stock.quote.history(symbol=symbol, start=start, end=end, interval=interval)
            if not a.empty:
                a.columns = [col.capitalize() for col in a.columns]
                a['ticker'] = symbol
                parts.append(a)
        except: pass
        
        # Nghỉ nhẹ mỗi 50 mã khi tải history
        if idx % 50 == 0: 
            print(f"   ... Đã tải {idx} mã...")
            time.sleep(20) 
            
    if parts:
        df = pd.concat(parts, ignore_index=True).set_index('Time')
        return df
    return pd.DataFrame()

def download_data():
    start = str(dt.date.today() - relativedelta(months=2))
    end = str(dt.date.today() - dt.timedelta(days=1))
    
    # Tải Minute
    df_minute = get_stock_price(tickers=cp, start=start, end=end, interval='1m', time_sleep=30)
    # Tải Day
    df_day = get_stock_price(tickers=cp, start=start, end=end, interval='1D', time_sleep=30)
    
    print("✅ Hoàn tất tải dữ liệu lịch sử!")
    return df_minute, df_day

# --- MAIN LOOP ---
def main():
    # 1. TẢI DATA (Chỉ chạy 1 lần khi bắt đầu)
    df_minute_hist, df_day_hist = download_data()
    
    print("🚀 BOT ĐÃ SẴN SÀNG! ĐANG CHỜ GIỜ GIAO DỊCH...")
    
    while True:
        now = datetime.now(VN_TZ)
        current_hm = now.hour * 100 + now.minute
        
        # LOGIC CHẠY THEO GIỜ
        is_trading_time = (915 <= current_hm < 1130) or (1300 <= current_hm < 1500)
        
        if is_trading_time:
            # QUÉT MỘT LƯỢT HẾT DANH SÁCH
            print(f"[{now.strftime('%H:%M:%S')}] 🔄 Bắt đầu vòng quét mới...", end='\r')
            
            for idx, symbol in enumerate(cp, start=1):
                # Check giờ liên tục (để nghỉ trưa đúng giờ)
                chk_now = datetime.now(VN_TZ)
                chk_hm = chk_now.hour * 100 + chk_now.minute
                if (1130 <= chk_hm < 1300) or (chk_hm >= 1500):
                    break # Thoát vòng for ngay
                
                # Gọi hàm check
                notification(symbol, df_day_hist, df_minute_hist, price_threshold=100000)
                
                # Nghỉ cực ngắn 1s giữa các mã để không bị API chặn (Intraday request)
                # Đây là thay thế cho việc ngủ 60s mỗi 50 mã. Cách này mượt hơn.
                time.sleep(1) 
            
            # Quét xong hết 1 vòng list CP -> Nghỉ 30s rồi quét lại từ đầu
            # Để tránh spam liên tục vào server nếu list ngắn
            if is_trading_time:
                # print("💤 Nghỉ 30s trước vòng quét tiếp theo...")
                time.sleep(30)
                
        # LOGIC TẮT MÁY / NGHỈ TRƯA
        elif 1130 <= current_hm < 1200:
            print("\n🍱 Hết phiên sáng. Bot tạm dừng (Action sẽ tự tắt sau đó).")
            break
        elif current_hm >= 1500:
            print("\n🏁 Hết phiên chiều. Bot kết thúc.")
            break
        else:
            # Giờ nghỉ trưa hoặc chờ đầu giờ
            print(f"[{now.strftime('%H:%M')}] Đang chờ...", end='\r')
            time.sleep(30)

if __name__ == "__main__":
    main()